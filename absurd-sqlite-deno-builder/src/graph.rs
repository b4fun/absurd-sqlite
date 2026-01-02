use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use deno_ast::ModuleSpecifier;
use deno_error::JsErrorBox;
use deno_graph::source::{LoadError, LoadResponse, ResolveError};
use import_map::ImportMap;

#[derive(Debug)]
pub struct SourceResolver(pub ImportMap);

impl deno_graph::source::Resolver for SourceResolver {
    fn resolve(
        &self,
        specifier: &str,
        referrer_range: &deno_graph::Range,
        _kind: deno_graph::source::ResolutionKind,
    ) -> Result<deno_graph::ModuleSpecifier, ResolveError> {
        self.0
            .resolve(specifier, &referrer_range.specifier)
            .map_err(ResolveError::ImportMap)
    }
}

fn load_error(message: impl Into<String>) -> LoadError {
    LoadError::Other(Arc::new(JsErrorBox::generic(message.into())))
}

fn load_file_with_root(
    file_source_root: &Path,
    specifier: &ModuleSpecifier,
) -> Result<Option<LoadResponse>, LoadError> {
    let file_path = specifier
        .to_file_path()
        .map_err(|_| load_error(format!("invalid file specifier {}", specifier)))?;
    let root = file_source_root
        .canonicalize()
        .map_err(|err| load_error(format!("failed to resolve file source root: {}", err)))?;
    let mut file_path = file_path;
    if !file_path.starts_with(&root)
        && let Ok(rel) = file_path.strip_prefix(Path::new("/"))
    {
        if rel
            .components()
            .any(|component| component == Component::ParentDir)
        {
            return Err(load_error(format!(
                "file is outside file source root: {}",
                specifier
            )));
        }
        file_path = root.join(rel);
    }

    if !file_path.starts_with(&root) {
        return Err(load_error(format!(
            "file is outside file source root: {}",
            specifier
        )));
    }

    let rel_path = file_path.strip_prefix(&root).map_err(|_| {
        load_error(format!(
            "failed to build virtual specifier for {}",
            specifier
        ))
    })?;
    if rel_path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(load_error(format!(
            "file is outside file source root: {}",
            specifier
        )));
    }
    let virtual_path = format!(
        "/{}",
        rel_path
            .to_string_lossy()
            .replace('\\', "/")
            .trim_start_matches('/')
    );
    let virtual_specifier = ModuleSpecifier::parse(&format!("file://{}", virtual_path))
        .map_err(|err| load_error(format!("failed to parse virtual specifier: {}", err)))?;

    let content = fs::read(&file_path)
        .map_err(|err| load_error(format!("failed to read {}: {}", file_path.display(), err)))?;

    Ok(Some(LoadResponse::Module {
        specifier: virtual_specifier,
        maybe_headers: None,
        mtime: None,
        content: Arc::from(content),
    }))
}

async fn load_from_http_remote(
    specifier: &ModuleSpecifier,
) -> Result<Option<LoadResponse>, LoadError> {
    let resp = reqwest::get(specifier.as_str())
        .await
        .map_err(|err| load_error(err.to_string()))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        Ok(None)
    } else {
        let resp = resp
            .error_for_status()
            .map_err(|err| load_error(err.to_string()))?;
        let mut headers = HashMap::new();
        for key in resp.headers().keys() {
            let key_str = key.to_string();
            let values = resp.headers().get_all(key);
            let values_str = values
                .iter()
                .filter_map(|e| e.to_str().ok())
                .collect::<Vec<&str>>()
                .join(",");
            headers.insert(key_str, values_str);
        }
        let url = resp.url().clone();
        let content = resp
            .bytes()
            .await
            .map_err(|err| load_error(err.to_string()))?;
        Ok(Some(deno_graph::source::LoadResponse::Module {
            specifier: url,
            mtime: None,
            maybe_headers: Some(headers),
            content: Arc::from(content.as_ref()),
        }))
    }
}

pub struct SourceLoader {
    pub file_source_root: PathBuf,
}

impl deno_graph::source::Loader for SourceLoader {
    fn load(
        &self,
        specifier: &deno_graph::ModuleSpecifier,
        _options: deno_graph::source::LoadOptions,
    ) -> deno_graph::source::LoadFuture {
        let specifier = specifier.clone();
        let file_source_root = self.file_source_root.clone();

        println!("source loader: loading module: {}", specifier);

        Box::pin(async move {
            match specifier.scheme() {
                "file" => load_file_with_root(file_source_root.as_path(), &specifier),
                "https" | "http" => load_from_http_remote(&specifier).await, // TODO: maintain an allowlist
                _ => Err(load_error(format!(
                    "unsupported scheme: {}",
                    specifier.scheme()
                ))),
            }
        })
    }
}
