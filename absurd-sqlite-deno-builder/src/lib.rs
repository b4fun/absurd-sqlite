use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use eszip::{EszipRelativeFileBaseUrl, EszipV2};
use url::Url;

use crate::graph::{SourceLoader, SourceResolver};

mod graph;

const IMPORT_MAP_FILE_NAME: &str = "deno.json";
const MAIN_TS_FILE_NAME: &str = "main.ts";

#[derive(Debug)]
pub struct ImportMapWithContent(import_map::ImportMap, String);

#[derive(Debug)]
pub struct EszipBuilderOptions {
    pub file_source_root: PathBuf,
    pub file_source_base_url: Url,
    pub main_file_url: Url,
    pub import_map: ImportMapWithContent,
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

    let mut graph = deno_graph::ModuleGraph::new(deno_graph::GraphKind::CodeOnly);
    graph
        .build(
            vec![options.main_file_url],
            Vec::new(),
            &source_loader,
            deno_graph::BuildOptions {
                resolver: Some(&source_resolver),
                module_analyzer: &analyzer,
                // TODO: npm resolver
                ..Default::default()
            },
        )
        .await;
    graph.valid()?;

    let mut eszip = EszipV2::from_graph(eszip::FromGraphOptions {
        graph,
        parser: analyzer.as_capturing_parser(),
        module_kind_resolver: Default::default(),
        transpile_options: deno_ast::TranspileOptions::default(),
        emit_options: deno_ast::EmitOptions::default(),
        relative_file_base: Some(EszipRelativeFileBaseUrl::new(&options.file_source_base_url)),
        npm_packages: None,
        npm_snapshot: Default::default(),
    })?;

    eszip.add_import_map(
        eszip::ModuleKind::Json,
        format!("file:///{IMPORT_MAP_FILE_NAME}"),
        options.import_map.1.as_bytes().into(),
    );

    Ok(eszip)
}
