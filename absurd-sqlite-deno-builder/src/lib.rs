use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use deno_error::{JsErrorBox, JsErrorClass};
use deno_npm::registry::{NpmPackageInfo, NpmRegistryApi, NpmRegistryPackageInfoLoadError};
use deno_npm::resolution::{
    AddPkgReqsOptions, DefaultTarballUrlProvider, NpmRegistryDefaultTarballUrlProvider,
    NpmResolutionSnapshot, NpmVersionResolver, ValidSerializedNpmResolutionSnapshot,
};
use deno_semver::npm::NpmPackageNvReference;
use deno_semver::package::{PackageNv, PackageReq};
use eszip::v2::FromGraphNpmPackages;
use eszip::{EszipRelativeFileBaseUrl, EszipV2};
use flate2::read::GzDecoder;
use tar::Archive;
use url::Url;

use crate::graph::{SourceLoader, SourceResolver};

mod graph;

const IMPORT_MAP_FILE_NAME: &str = "deno.json";
const MAIN_TS_FILE_NAME: &str = "main.ts";
type NpmPackageJsons = Vec<(String, Vec<u8>)>;
type NpmPackageModules = Vec<(NpmPackageNvReference, (String, Vec<u8>))>;

#[derive(Debug)]
pub struct ImportMapWithContent(import_map::ImportMap, String);

#[derive(Debug)]
pub struct EszipBuilderOptions {
    pub file_source_root: PathBuf,
    pub file_source_base_url: Url,
    pub main_file_url: Url,
    pub import_map: ImportMapWithContent,
}

#[derive(Debug, Clone)]
struct NpmRegistryHttpClient {
    client: reqwest::Client,
    cache: Arc<Mutex<HashMap<String, Arc<NpmPackageInfo>>>>,
}

impl NpmRegistryHttpClient {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn package_url(package_name: &str) -> String {
        let encoded = package_name.replace('/', "%2F");
        format!("https://registry.npmjs.org/{}", encoded)
    }

    async fn fetch_tarball(&self, tarball_url: &str) -> Result<Vec<u8>> {
        let response = self.client.get(tarball_url).send().await?;
        let response = response.error_for_status()?;
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }
}

fn npm_registry_error(err: impl std::fmt::Display) -> NpmRegistryPackageInfoLoadError {
    NpmRegistryPackageInfoLoadError::LoadError(Arc::new(JsErrorBox::generic(err.to_string())))
}

#[async_trait(?Send)]
impl NpmRegistryApi for NpmRegistryHttpClient {
    async fn package_info(
        &self,
        name: &str,
    ) -> std::result::Result<Arc<NpmPackageInfo>, NpmRegistryPackageInfoLoadError> {
        if let Some(info) = self.cache.lock().unwrap().get(name).cloned() {
            return Ok(info);
        }

        let url = Self::package_url(name);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(npm_registry_error)?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(NpmRegistryPackageInfoLoadError::PackageNotExists {
                package_name: name.to_string(),
            });
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(npm_registry_error(format!(
                "npm registry error {}: {}",
                status, body
            )));
        }

        let info = response
            .json::<NpmPackageInfo>()
            .await
            .map_err(npm_registry_error)?;
        let info = Arc::new(info);
        self.cache
            .lock()
            .unwrap()
            .insert(name.to_string(), info.clone());
        Ok(info)
    }
}

#[derive(Debug)]
struct NpmResolver {
    registry: NpmRegistryHttpClient,
    version_resolver: NpmVersionResolver,
    snapshot: Arc<Mutex<NpmResolutionSnapshot>>,
}

impl NpmResolver {
    fn new() -> Self {
        Self {
            registry: NpmRegistryHttpClient::new(),
            version_resolver: NpmVersionResolver::default(),
            snapshot: Arc::new(Mutex::new(NpmResolutionSnapshot::default())),
        }
    }

    fn snapshot(&self) -> ValidSerializedNpmResolutionSnapshot {
        self.snapshot.lock().unwrap().as_valid_serialized()
    }

    async fn npm_packages(&self) -> Result<FromGraphNpmPackages> {
        let snapshot = self.snapshot.lock().unwrap().clone();
        let mut npm_packages = FromGraphNpmPackages::new();
        let mut seen = HashSet::new();

        for package in snapshot.all_packages_for_every_system() {
            let nv = package.id.nv.clone();
            if !seen.insert(nv.clone()) {
                continue;
            }
            let info = self.registry.package_info(nv.name.as_str()).await?;
            let version_info = info
                .version_info(&nv, self.version_resolver.link_packages.as_ref())
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            let tarball_url = version_info
                .dist
                .as_ref()
                .map(|dist| dist.tarball.clone())
                .unwrap_or_else(|| NpmRegistryDefaultTarballUrlProvider.default_tarball_url(&nv));
            let tarball_bytes = self.registry.fetch_tarball(&tarball_url).await?;
            let (package_jsons, modules) = extract_npm_package_files(&nv, &tarball_bytes)?;
            npm_packages.add_package(nv, package_jsons, modules);
        }

        Ok(npm_packages)
    }
}

#[async_trait(?Send)]
impl deno_graph::source::NpmResolver for NpmResolver {
    fn load_and_cache_npm_package_info(&self, package_name: &str) {
        let _ = package_name;
    }

    async fn resolve_pkg_reqs(
        &self,
        package_reqs: &[PackageReq],
    ) -> deno_graph::NpmResolvePkgReqsResult {
        let (snapshot, fallback_snapshot) = {
            let mut guard = self.snapshot.lock().unwrap();
            let snapshot = std::mem::take(&mut *guard);
            let fallback_snapshot = snapshot.clone();
            (snapshot, fallback_snapshot)
        };

        let add_result = snapshot
            .add_pkg_reqs(
                &self.registry,
                AddPkgReqsOptions {
                    package_reqs,
                    version_resolver: &self.version_resolver,
                    should_dedup: true,
                },
                None,
            )
            .await;

        let dep_graph_result: std::result::Result<(), Arc<dyn JsErrorClass>> =
            match &add_result.dep_graph_result {
                Ok(_) => Ok(()),
                Err(err) => Err(Arc::new(err.clone()) as Arc<dyn JsErrorClass>),
            };

        {
            let mut guard = self.snapshot.lock().unwrap();
            if let Ok(snapshot) = &add_result.dep_graph_result {
                *guard = snapshot.clone();
            } else {
                *guard = fallback_snapshot;
            }
        }

        let results = add_result
            .results
            .into_iter()
            .map(|result| {
                result.map_err(|err| deno_graph::NpmLoadError::PackageReqResolution(Arc::new(err)))
            })
            .collect::<Vec<Result<PackageNv, deno_graph::NpmLoadError>>>();

        deno_graph::NpmResolvePkgReqsResult {
            results,
            dep_graph_result,
        }
    }
}

fn add_exact_version_root_packages(
    snapshot: ValidSerializedNpmResolutionSnapshot,
) -> ValidSerializedNpmResolutionSnapshot {
    // Eszip looks up npm packages in the snapshot by exact version reqs derived
    // from resolved npm: specifiers. The snapshot only stores the original
    // version requirements, so add exact-version keys to keep lookups working.
    let mut serialized = snapshot.into_serialized();
    let root_packages = serialized.root_packages.clone();
    for (_, id) in root_packages {
        let exact_req = PackageReq::from_str(&format!("{}@{}", id.nv.name, id.nv.version)).unwrap();
        serialized.root_packages.entry(exact_req).or_insert(id);
    }
    serialized.into_valid_unsafe()
}

fn extract_npm_package_files(
    nv: &PackageNv,
    tarball: &[u8],
) -> Result<(NpmPackageJsons, NpmPackageModules)> {
    let decoder = GzDecoder::new(tarball);
    let mut archive = Archive::new(decoder);
    let mut package_jsons = Vec::new();
    let mut modules = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path()?;
        let Some(rel_path) = normalize_npm_entry_path(&path) else {
            continue;
        };
        let mut content = Vec::new();
        entry.read_to_end(&mut content)?;
        let specifier = format!("npm:{}@{}/{}", nv.name, nv.version, rel_path);
        if rel_path == "package.json" {
            package_jsons.push((specifier, content));
        } else {
            let nv_ref = NpmPackageNvReference::from_str(&specifier)
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            modules.push((nv_ref, (specifier, content)));
        }
    }

    Ok((package_jsons, modules))
}

fn normalize_npm_entry_path(path: &Path) -> Option<String> {
    let rel_path = match path.strip_prefix("package") {
        Ok(path) => path,
        Err(_) => path,
    };
    let rel_path = rel_path.strip_prefix(Path::new("/")).unwrap_or(rel_path);
    if rel_path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return None;
    }
    let rel_path = rel_path.to_string_lossy().replace('\\', "/");
    let rel_path = rel_path.trim_start_matches('/').to_string();
    if rel_path.is_empty() {
        None
    } else {
        Some(rel_path)
    }
}

fn read_import_map_from_file(base_url: Url, file_path: &PathBuf) -> Result<ImportMapWithContent> {
    let import_map_content = fs::read_to_string(file_path)?;
    let import_map = import_map::parse_from_json(base_url, &import_map_content)?;
    Ok(ImportMapWithContent(
        import_map.import_map,
        import_map_content,
    ))
}

impl EszipBuilderOptions {
    pub fn from_dir(file_source_root: PathBuf) -> Result<Self> {
        let file_source_root = file_source_root.canonicalize()?;
        let file_source_base_url = Url::from_directory_path(&file_source_root)
            .map_err(|_| anyhow::anyhow!("invalid file source root path"))?;
        let main_file_url = file_source_base_url
            .join(MAIN_TS_FILE_NAME)
            .map_err(|_| anyhow::anyhow!("failed to construct main file url"))?;

        let deno_file_path = file_source_root.join(IMPORT_MAP_FILE_NAME);
        let import_map = read_import_map_from_file(file_source_base_url.clone(), &deno_file_path)?;

        Ok(EszipBuilderOptions {
            file_source_root,
            file_source_base_url,
            main_file_url,
            import_map,
        })
    }
}

pub async fn build_eszip(options: EszipBuilderOptions) -> Result<EszipV2> {
    let source_resolver = SourceResolver(options.import_map.0);
    let source_loader = SourceLoader {
        file_source_root: options.file_source_root,
    };
    let analyzer = deno_graph::ast::CapturingModuleAnalyzer::default();
    let npm_resolver = NpmResolver::new();

    let mut graph = deno_graph::ModuleGraph::new(deno_graph::GraphKind::CodeOnly);
    graph
        .build(
            vec![options.main_file_url],
            Vec::new(),
            &source_loader,
            deno_graph::BuildOptions {
                resolver: Some(&source_resolver),
                module_analyzer: &analyzer,
                npm_resolver: Some(&npm_resolver),
                ..Default::default()
            },
        )
        .await;
    graph.valid()?;

    let npm_snapshot = add_exact_version_root_packages(npm_resolver.snapshot());
    let npm_packages = npm_resolver.npm_packages().await?;

    let mut eszip = EszipV2::from_graph(eszip::FromGraphOptions {
        graph,
        parser: analyzer.as_capturing_parser(),
        module_kind_resolver: Default::default(),
        transpile_options: deno_ast::TranspileOptions::default(),
        emit_options: deno_ast::EmitOptions::default(),
        relative_file_base: Some(EszipRelativeFileBaseUrl::new(&options.file_source_base_url)),
        npm_packages: Some(npm_packages),
        npm_snapshot,
    })?;

    eszip.add_import_map(
        eszip::ModuleKind::Json,
        format!("file:///{IMPORT_MAP_FILE_NAME}"),
        options.import_map.1.as_bytes().into(),
    );

    Ok(eszip)
}
