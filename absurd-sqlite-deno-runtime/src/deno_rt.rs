use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

use anyhow::Result;
use deno_ast::parse_module;
use deno_ast::{MediaType, ParseParams};
use deno_core::error::{AnyError, CoreError};
use deno_core::{
    ModuleCodeString, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleSource,
    ModuleSourceCode, ModuleSpecifier, ModuleType, ResolutionKind,
};
use deno_error::JsErrorBox;
use deno_npm::resolution::NpmResolutionSnapshot;
use deno_npm::resolution::ValidSerializedNpmResolutionSnapshot;
use deno_semver::VersionReq;
use deno_semver::npm::{NpmPackageNvReference, NpmPackageReqReference};
use deno_semver::package::PackageNv;
use futures::io::BufReader;
use import_map::ImportMap;
use serde_json::Value;
use url::Url;

const DENO_MAIN_ESZIP: &'static [u8] = include_bytes!("../deno-main.eszip2");

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
    base: Url,
    maybe_import_map: Option<ImportMap>,
    map: HashMap<ModuleSpecifier, ModuleCodeString>,
    npm_snapshot: Option<ValidSerializedNpmResolutionSnapshot>,
    npm_resolution: Option<NpmResolutionSnapshot>,
}

impl ModuleLoader {
    async fn new_from_eszip() -> Result<Self> {
        let eszip_reader = BufReader::new(DENO_MAIN_ESZIP);

        let (mut eszip, loader) = eszip::EszipV2::parse(eszip_reader).await?;
        let npm_snapshot = eszip.take_npm_snapshot();
        let npm_resolution = npm_snapshot
            .as_ref()
            .map(|snapshot| NpmResolutionSnapshot::new(snapshot.clone()));

        let fut = async move {
            let mut map: HashMap<ModuleSpecifier, ModuleCodeString> = HashMap::new();

            for (specifier, module) in eszip {
                if module.specifier != specifier {
                    continue;
                }
                if specifier.starts_with("data:") {
                    continue;
                }

                println!("loading eszip module: {}", specifier);

                let source = module.source().await.expect("source already taken");
                let source = std::str::from_utf8(&source).unwrap();

                let specifier = Url::parse(&specifier).unwrap(); // FIXME

                map.insert(
                    specifier.clone(),
                    ModuleCodeString::from(source.to_string())
                        .into_cheap_copy()
                        .0,
                );
            }

            Ok(map)
        };

        let (_, map) = tokio::try_join!(loader, fut)?;

        let import_map_specifier =
            Url::parse("file:///deno.json").map_err(|err| JsErrorBox::generic(err.to_string()))?;

        let import_map = if let Some(code) = map.get(&import_map_specifier) {
            let import_map_content = code.to_string();
            let import_map = import_map::parse_from_json(
                Url::parse("file:///").map_err(|err| JsErrorBox::generic(err.to_string()))?,
                &import_map_content,
            )
            .map_err(|err| JsErrorBox::generic(err.to_string()))?;
            Some(import_map.import_map)
        } else {
            None
        };

        let base = Url::parse("file:///")?;

        Ok(Self {
            base,
            maybe_import_map: import_map,
            map,
            npm_snapshot,
            npm_resolution,
        })
    }

    fn add_module(&mut self, specifier: ModuleSpecifier, code: ModuleCodeString) -> &mut Self {
        self.map.insert(specifier, code);
        self
    }

    fn parse_referrer(&self, referrer: &str) -> Result<Url, JsErrorBox> {
        match Url::parse(referrer) {
            Ok(url) => Ok(url),
            Err(url::ParseError::RelativeUrlWithoutBase) => self
                .base
                .join(referrer)
                .map_err(|err| JsErrorBox::generic(err.to_string())),
            Err(err) => Err(JsErrorBox::generic(err.to_string())),
        }
    }

    fn try_resolve_jsr_specifier(&self, specifier: &str) -> Option<ModuleSpecifier> {
        let specifier = specifier.strip_prefix("jsr:")?;
        let (package, version) = specifier.rsplit_once('@')?;
        let version = version.strip_prefix('^').unwrap_or(version);
        if package.is_empty() || version.is_empty() {
            return None;
        }
        let url = format!("https://jsr.io/{}/{}/mod.ts", package, version);
        Url::parse(&url).ok()
    }

    fn try_resolve_npm_specifier(&self, specifier: &str) -> Option<ModuleSpecifier> {
        if !specifier.starts_with("npm:") {
            return None;
        }
        let req_ref = NpmPackageReqReference::from_str(specifier).ok()?;
        let resolved_nv = self
            .npm_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.as_serialized().root_packages.get(req_ref.req()))
            .map(|id| id.nv.clone())
            .or_else(|| {
                NpmPackageNvReference::from_str(specifier)
                    .ok()
                    .map(|nv_ref| nv_ref.nv().clone())
            })?;
        let sub_path = req_ref
            .sub_path()
            .map(|path| path.trim_start_matches('/').to_string());
        let entry_path = match sub_path {
            Some(path) if !path.is_empty() => path,
            _ => self
                .resolve_npm_entry_from_package_json(&resolved_nv)
                .unwrap_or_else(|| "index.js".to_string()),
        };
        let specifier = format!(
            "npm:{}@{}/{}",
            resolved_nv.name, resolved_nv.version, entry_path
        );
        Url::parse(&specifier).ok()
    }

    fn try_resolve_npm_dependency_specifier(
        &self,
        specifier: &str,
        referrer: &str,
    ) -> Option<ModuleSpecifier> {
        if let Some(resolved) = try_resolve_node_builtin(specifier) {
            return Some(resolved);
        }
        if !is_bare_specifier(specifier) {
            return None;
        }
        let referrer_nv = NpmPackageNvReference::from_str(referrer).ok()?;
        let resolution = self.npm_resolution.as_ref()?;
        let (package_name, sub_path) = parse_bare_specifier(specifier)?;
        let package_id = resolution.package_ids_for_nv(referrer_nv.nv()).next()?;
        let package = resolution.package_from_id(package_id)?;

        let dep_id = package
            .dependencies
            .get(package_name.as_str())
            .cloned()
            .or_else(|| {
                let any_version = VersionReq::parse_from_npm("*").ok()?;
                resolution.resolve_best_package_id(package_name.as_str(), &any_version)
            })?;

        let entry_path = match sub_path {
            Some(path) if !path.is_empty() => path,
            _ => self
                .resolve_npm_entry_from_package_json(&dep_id.nv)
                .unwrap_or_else(|| "index.js".to_string()),
        };
        let specifier = format!(
            "npm:{}@{}/{}",
            dep_id.nv.name, dep_id.nv.version, entry_path
        );
        Url::parse(&specifier).ok()
    }

    fn resolve_npm_entry_from_package_json(&self, nv: &PackageNv) -> Option<String> {
        let pkg_json_specifier =
            Url::parse(&format!("npm:{}@{}/package.json", nv.name, nv.version)).ok()?;
        let code = self.map.get(&pkg_json_specifier)?;
        let json: Value = serde_json::from_str(code.as_ref()).ok()?;

        if let Some(entry) = extract_exports_entry(&json) {
            return Some(entry);
        }

        let module = json.get("module").and_then(|value| value.as_str());
        if let Some(entry) = module {
            return Some(entry.to_string());
        }

        let main = json.get("main").and_then(|value| value.as_str());
        main.map(|entry| entry.to_string())
    }

    fn try_resolve_from_import_map(
        &self,
        specifier: &str,
        referrer: &str,
    ) -> Option<ModuleSpecifier> {
        if self.maybe_import_map.is_none() {
            return None;
        }
        let import_map = self.maybe_import_map.as_ref().unwrap();

        let referrer = match self.parse_referrer(referrer) {
            Ok(url) => url,
            Err(err) => {
                println!("  failed to parse referrer {}, err: {}", referrer, err);
                return None;
            }
        };

        match import_map.resolve(specifier, &referrer) {
            Ok(url) => Some(url),
            Err(err) => {
                println!(
                    "  failed to resolve specifier {} from referrer {}, err: {}",
                    specifier, referrer, err
                );
                None
            }
        }
    }

    fn try_load_code_from_specifier(
        &self,
        specifier: &ModuleSpecifier,
    ) -> Option<ModuleLoadResponse> {
        if let Some(code) = self.map.get(specifier) {
            return Some(ModuleLoadResponse::Sync(Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(code.try_clone().unwrap()),
                specifier,
                None,
            ))));
        }
        None
    }
}

impl deno_core::ModuleLoader for ModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, JsErrorBox> {
        println!(
            "resolving specifier {} from referrer {}",
            specifier, referrer
        );

        if let Some(resolved) = self.try_resolve_from_import_map(specifier, referrer) {
            println!("  resolved via import map to {}", resolved);
            if let Some(converted) = self.try_resolve_npm_specifier(resolved.as_str()) {
                println!("  resolved npm specifier to {}", converted);
                return Ok(converted);
            }
            return Ok(resolved);
        }

        if let Some(resolved) = self.try_resolve_npm_dependency_specifier(specifier, referrer) {
            println!("  resolved npm dependency specifier to {}", resolved);
            return Ok(resolved);
        }

        if let Some(resolved) = self.try_resolve_npm_specifier(specifier) {
            println!("  resolved npm specifier to {}", resolved);
            return Ok(resolved);
        }

        if let Some(resolved) = self.try_resolve_jsr_specifier(specifier) {
            println!("  resolved jsr specifier to {}", resolved);
            return Ok(resolved);
        }

        deno_core::resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        println!("loading {} {:?}", module_specifier, _maybe_referrer);

        // fast path
        if let Some(res) = self.try_load_code_from_specifier(module_specifier) {
            return res;
        }

        // slow path

        let res = if let Some(code) = self.map.get(module_specifier) {
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(code.try_clone().unwrap()),
                module_specifier,
                None,
            ))
        } else if let Some(resolved) = self.try_resolve_npm_specifier(module_specifier.as_str())
            && let Some(code) = self.map.get(&resolved)
        {
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(code.try_clone().unwrap()),
                &resolved,
                None,
            ))
        } else if let Some(resolved) = self.try_resolve_jsr_specifier(module_specifier.as_str())
            && let Some(code) = self.map.get(&resolved)
        {
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(code.try_clone().unwrap()),
                &resolved,
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

fn extract_exports_entry(json: &Value) -> Option<String> {
    let exports = json.get("exports")?;
    if let Some(entry) = exports.as_str() {
        let entry = normalize_package_json_entry(entry);
        return (!is_esm_entry(&entry)).then_some(entry);
    }
    let exports_obj = exports.as_object()?;
    let root = exports_obj.get(".")?;
    if let Some(entry) = root.as_str() {
        let entry = normalize_package_json_entry(entry);
        return (!is_esm_entry(&entry)).then_some(entry);
    }
    if let Some(conditions) = root.as_object() {
        if let Some(entry) = conditions.get("require").and_then(|value| value.as_str()) {
            let entry = normalize_package_json_entry(entry);
            if !is_esm_entry(&entry) {
                return Some(entry);
            }
        }
        if let Some(entry) = conditions.get("import").and_then(|value| value.as_str()) {
            let entry = normalize_package_json_entry(entry);
            if !is_esm_entry(&entry) {
                return Some(entry);
            }
        }
        if let Some(entry) = conditions.get("default").and_then(|value| value.as_str()) {
            let entry = normalize_package_json_entry(entry);
            if !is_esm_entry(&entry) {
                return Some(entry);
            }
        }
    }
    None
}

fn normalize_package_json_entry(entry: &str) -> String {
    entry
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string()
}

fn is_esm_entry(entry: &str) -> bool {
    entry.ends_with(".mjs") || entry.ends_with(".mts")
}

fn is_bare_specifier(specifier: &str) -> bool {
    if specifier.starts_with("./") || specifier.starts_with("../") || specifier.starts_with('/') {
        return false;
    }
    if specifier.contains("://") {
        return false;
    }
    if specifier.starts_with("npm:")
        || specifier.starts_with("jsr:")
        || specifier.starts_with("node:")
    {
        return false;
    }
    true
}

fn parse_bare_specifier(specifier: &str) -> Option<(String, Option<String>)> {
    if specifier.starts_with('@') {
        let mut parts = specifier.splitn(3, '/');
        let scope = parts.next()?;
        let name = parts.next()?;
        let package_name = format!("{}/{}", scope, name);
        let sub_path = parts.next().map(|rest| rest.to_string());
        return Some((package_name, sub_path));
    }

    let mut parts = specifier.splitn(2, '/');
    let package_name = parts.next()?.to_string();
    let sub_path = parts.next().map(|rest| rest.to_string());
    Some((package_name, sub_path))
}

fn try_resolve_node_builtin(specifier: &str) -> Option<ModuleSpecifier> {
    match specifier {
        "os" => Url::parse("node:os").ok(),
        _ => None,
    }
}

const DENO_MAIN_SOURCE: &'static str = include_str!("../deno-main/main.ts");

pub async fn run(user_module_path: &str) -> Result<(), AnyError> {
    // build_entrypoint_eszip().await

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

    let mut module_loader = ModuleLoader::new_from_eszip().await?;
    module_loader
        .add_module(
            user_module_specifier.clone(),
            ModuleCodeString::from(String::from_utf8(user_module_source)?)
                .into_cheap_copy()
                .0,
        )
        .add_module(
            main_specifier.clone(),
            ModuleCodeString::from(String::from_utf8(main_source)?)
                .into_cheap_copy()
                .0,
        );

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
