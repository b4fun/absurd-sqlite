use deno_ast::parse_module;
use deno_ast::{MediaType, ParseParams};
use deno_core::error::{AnyError, CoreError};
use deno_error::JsErrorBox;
use std::path::Path;
use std::rc::Rc;

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
    specifier: &deno_core::ModuleSpecifier,
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

struct StaticModuleLoader {
    real: deno_core::StaticModuleLoader,
    main_module: deno_core::ModuleSpecifier,
    bootstrap_module: deno_core::ModuleSpecifier,
}

impl deno_core::ModuleLoader for StaticModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: deno_core::ResolutionKind,
    ) -> Result<deno_core::ModuleSpecifier, deno_error::JsErrorBox> {
        println!(
            "Resolving module: specifier='{}', referrer='{}' kind={:?}",
            specifier, referrer, _kind
        );
        if specifier == "@absurd-sqlite/sdk" {
            return self
                .real
                .resolve("node:@absurd-sqlite/sdk", referrer, _kind);
        }
        if specifier == self.main_module.as_str() && referrer == self.bootstrap_module.as_str() {
            return Ok(self.main_module.clone());
        }
        if specifier == self.bootstrap_module.as_str() {
            return Ok(self.bootstrap_module.clone());
        }

        Err(JsErrorBox::generic(
            "Imports are limited to the main module and @absurd-sqlite/sdk",
        ))
    }

    fn load(
        &self,
        module_specifier: &deno_core::ModuleSpecifier,
        _maybe_referrer: Option<&deno_core::ModuleLoadReferrer>,
        options: deno_core::ModuleLoadOptions,
    ) -> deno_core::ModuleLoadResponse {
        self.real.load(module_specifier, _maybe_referrer, options)
    }
}

async fn run_js(file_path: &str) -> Result<(), AnyError> {
    let main_module = deno_core::resolve_path(file_path, &std::env::current_dir()?)?;
    let source = std::fs::read_to_string(file_path)?;
    let (main_media_type, _main_module_type, main_should_transpile) =
        media_type_info(Path::new(file_path))?;
    let source = if main_should_transpile {
        let transpiled =
            transpile_module_source(&main_module, source, main_media_type, main_should_transpile)?;
        String::from_utf8(transpiled).map_err(|err| JsErrorBox::generic(err.to_string()))?
    } else {
        source
    };

    let fake_module = url::Url::parse("node:@absurd-sqlite/sdk")?;
    let fake_source = r#"
        export class Absurd {
            constructor() {
                this.version = "fake-version";
                this.info = "This is a fake Absurd module for testing.";
            }

            async start() {
                return;
            }
        }
    "#;
    print!("Fake module specifier: {}\n", fake_module);

    let main_specifier = main_module.as_str().to_string();
    let bootstrap_module = url::Url::parse("absurd:bootstrap")?;
    let bootstrap_source = format!(
        r#"
        import {{ Absurd }} from "@absurd-sqlite/sdk";
        import main from {main_specifier:?};

        if (typeof main !== "function") {{
            throw new Error("Main module must export a default function (absurd: Absurd) => void");
        }}

        const absurd = new Absurd();
        await Promise.resolve(main(absurd));

        if (typeof absurd.start !== "function") {{
            throw new Error("Absurd client must expose a start() function");
        }}

        await absurd.start();
        "#
    );

    let module_loader = StaticModuleLoader {
        real: deno_core::StaticModuleLoader::new([
            (bootstrap_module.clone(), bootstrap_source),
            (main_module.clone(), source.clone()),
            (fake_module.clone(), fake_source.to_string()),
        ]),
        main_module: main_module.clone(),
        bootstrap_module: bootstrap_module.clone(),
    };

    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(module_loader)),
        ..Default::default()
    });

    let mod_id = js_runtime.load_main_es_module(&bootstrap_module).await?;
    let result = js_runtime.mod_evaluate(mod_id);
    js_runtime.run_event_loop(Default::default()).await?;

    match result.await {
        Ok(_) => Ok(()),
        Err(err) => Err(CoreError::from(err).into()),
    }
}

fn main() {
    let script_path = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            eprintln!("usage: absurd-deno-runtime <script>");
            std::process::exit(1);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Err(error) = runtime.block_on(run_js(&script_path)) {
        eprintln!("error: {}", error);
    }
}
