use deno_ast::parse_module;
use deno_ast::{MediaType, ParseParams};
use deno_core::error::{AnyError, CoreError};
use deno_core::{
    ModuleCodeBytes, ModuleCodeString, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse,
    ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType, ResolutionKind,
};
use deno_error::JsErrorBox;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use url::Url;

fn media_type_info(path: &Path) -> Result<(MediaType, deno_core::ModuleType, bool), JsErrorBox> {
    let media_type = MediaType::from_path(path);
    let (module_type, should_transpile) = match media_type {
        MediaType::JavaScript | MediaType::Mjs | MediaType::Cjs => {
            (deno_core::ModuleType::JavaScript, false)
        }
        MediaType::Jsx => (deno_core::ModuleType::JavaScript, true),
        MediaType::TypeScript
        | MediaType::Mts
        | MediaType::Cts
        | MediaType::Dts
        | MediaType::Dmts
        | MediaType::Dcts
        | MediaType::Tsx => (deno_core::ModuleType::JavaScript, true),
        MediaType::Json => (deno_core::ModuleType::Json, false),
        _ => {
            return Err(JsErrorBox::generic(format!(
                "Unknown extension {:?}",
                path.extension()
            )));
        }
    };
    Ok((media_type, module_type, should_transpile))
}

fn transpile_module_source(
    specifier: &ModuleSpecifier,
    code: String,
    media_type: MediaType,
    should_transpile: bool,
) -> Result<Vec<u8>, JsErrorBox> {
    if !should_transpile {
        return Ok(code.into_bytes());
    }

    let parsed = parse_module(ParseParams {
        specifier: specifier.clone(),
        text: code.into(),
        media_type,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|err| JsErrorBox::generic(err.to_string()))?;
    let transpiled = parsed
        .transpile(
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .map_err(|err| JsErrorBox::generic(err.to_string()))?
        .into_source();
    Ok(transpiled.text.into_bytes())
}

struct ModuleLoader {
    map: HashMap<ModuleSpecifier, ModuleCodeString>,
}

impl deno_core::ModuleLoader for ModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, JsErrorBox> {
        println!("resolving {} from {}", specifier, referrer);

        let url = match Url::parse(specifier) {
            Ok(url) => url,
            Err(deno_core::url::ParseError::RelativeUrlWithoutBase) => {
                let base =
                    Url::parse(referrer).map_err(|err| JsErrorBox::generic(err.to_string()))?;
                base.join(specifier)
                    .map_err(|err| JsErrorBox::generic(err.to_string()))?
            }
            Err(err) => {
                return Err(JsErrorBox::generic(err.to_string()));
            }
        };

        Ok(url)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        println!("loading {}", module_specifier);

        let res = if let Some(code) = self.map.get(module_specifier) {
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(code.try_clone().unwrap()),
                module_specifier,
                None,
            ))
        } else {
            Err(JsErrorBox::generic(format!(
                "Module not found: {}",
                module_specifier
            )))
        };
        ModuleLoadResponse::Sync(res)
    }
}

const DENO_MAIN_SOURCE: &'static str = include_str!("./deno-main/main.ts");

pub async fn run(user_module_path: &str) -> Result<(), AnyError> {
    let main_specifier = ModuleSpecifier::parse("file:main.ts")?;
    let main_source = transpile_module_source(
        &main_specifier,
        DENO_MAIN_SOURCE.to_string(),
        MediaType::TypeScript,
        true,
    )?;

    println!("parsed main source: {}", main_specifier.as_str());

    let user_module_specifier =
        deno_core::resolve_path(user_module_path, &std::env::current_dir()?)?;
    let user_module_code = std::fs::read_to_string(user_module_path)?;
    let (media_type, _module_type, should_transpile) =
        media_type_info(Path::new(user_module_path))?;
    let user_module_source = transpile_module_source(
        &user_module_specifier,
        user_module_code,
        media_type,
        should_transpile,
    )?;

    println!(
        "parsed user module source: {:?} {:?}",
        user_module_specifier.as_str(),
        ModuleCodeString::from(String::from_utf8(user_module_source.clone())?),
    );

    let module_loader = ModuleLoader {
        map: HashMap::from([
            (
                main_specifier.clone(),
                ModuleCodeString::from(String::from_utf8(main_source)?)
                    .into_cheap_copy()
                    .0,
            ),
            (
                user_module_specifier.clone(),
                ModuleCodeString::from(String::from_utf8(user_module_source)?)
                    .into_cheap_copy()
                    .0,
            ),
        ]),
    };

    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(module_loader)),
        ..Default::default()
    });

    let mod_id = js_runtime.load_main_es_module(&main_specifier).await?;
    let result = js_runtime.mod_evaluate(mod_id);
    js_runtime.run_event_loop(Default::default()).await?;

    match result.await {
        Ok(_) => Ok(()),
        Err(err) => Err(CoreError::from(err).into()),
    }
}
