use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FunctionKind {
    Scalar,
    Table,
}

#[derive(Debug, Clone)]
struct FunctionEntry {
    name: String,
    arities: BTreeSet<i32>,
    rust_target: Option<String>,
    description: Option<String>,
    signature: Option<String>,
    section: Option<DocSection>,
    rust_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DocSection {
    Durable,
    Schema,
    Meta,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let out_path = parse_out_path(&args);

    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let lib_source = fs::read_to_string(src_root.join("lib.rs"))?;

    let mut entries = BTreeMap::<(FunctionKind, String), FunctionEntry>::new();
    collect_scalar_functions(&lib_source, &mut entries);
    collect_table_functions(&lib_source, &mut entries);

    let docs = extract_doc_comments(&src_root)?;
    for entry in entries.values_mut() {
        if let Some(target) = entry.rust_target.as_ref() {
            if let Some(doc) = docs.get(target) {
                apply_doc_info(entry, doc);
                continue;
            }
            if let Some(short) = target.rsplit("::").next() {
                if let Some(doc) = docs.get(short) {
                    apply_doc_info(entry, doc);
                }
            }
        }
        if entry.description.is_none() {
            if let Some(rust_type) = entry.rust_type.as_ref() {
                if let Some(doc) = docs.get(rust_type) {
                    apply_doc_info(entry, doc);
                    continue;
                }
                if let Some(short) = rust_type.rsplit("::").next() {
                    if let Some(doc) = docs.get(short) {
                        apply_doc_info(entry, doc);
                    }
                }
            }
        }
    }

    let output = render_markdown(entries.values());

    match out_path {
        Some(path) => fs::write(path, output)?,
        None => println!("{output}"),
    }

    Ok(())
}

fn parse_out_path(args: &[String]) -> Option<PathBuf> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--out" {
            if let Some(value) = args.get(i + 1) {
                return Some(PathBuf::from(value));
            }
        }
        i += 1;
    }
    None
}

fn collect_scalar_functions(
    source: &str,
    entries: &mut BTreeMap<(FunctionKind, String), FunctionEntry>,
) {
    for call in find_calls(source, "define_scalar_function") {
        let args = match parse_call_args(call) {
            Some(args) => args,
            None => continue,
        };
        if args.len() < 4 {
            continue;
        }
        let name = match parse_string_literal(&args[1]) {
            Some(name) => name,
            None => continue,
        };
        let arity = match args[2].trim().parse::<i32>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let target = args[3].trim().to_string();
        let key = (FunctionKind::Scalar, name.clone());
        let entry = entries.entry(key).or_insert(FunctionEntry {
            name,
            arities: BTreeSet::new(),
            rust_target: None,
            description: None,
            signature: None,
            section: None,
            rust_type: None,
        });
        entry.arities.insert(arity);
        if entry.rust_target.is_none() {
            entry.rust_target = Some(target);
        }
    }
}

fn collect_table_functions(
    source: &str,
    entries: &mut BTreeMap<(FunctionKind, String), FunctionEntry>,
) {
    let mut cursor = 0;
    while let Some(idx) = source[cursor..].find("define_table_function::<") {
        let start = cursor + idx;
        let type_start = start + "define_table_function::<".len();
        let rest = &source[type_start..];
        let type_end = match rest.find('>') {
            Some(pos) => type_start + pos,
            None => break,
        };
        let rust_type = source[type_start..type_end].trim().to_string();
        let rust_type_copy = rust_type.clone();
        let after_type = &source[type_end + 1..];
        let open_paren = match after_type.find('(') {
            Some(pos) => type_end + 1 + pos,
            None => break,
        };
        let call_source = &source[open_paren..];
        let args = match parse_call_args(call_source) {
            Some(args) => args,
            None => break,
        };
        if args.len() < 2 {
            cursor = open_paren + 1;
            continue;
        }
        let name = match parse_string_literal(&args[1]) {
            Some(name) => name,
            None => {
                cursor = open_paren + 1;
                continue;
            }
        };
        let key = (FunctionKind::Table, name.clone());
        let entry = entries.entry(key).or_insert(FunctionEntry {
            name,
            arities: BTreeSet::new(),
            rust_target: None,
            description: None,
            signature: None,
            section: None,
            rust_type: Some(rust_type),
        });
        if entry.rust_type.is_none() {
            entry.rust_type = Some(rust_type_copy);
        }
        cursor = open_paren + 1;
    }
}

fn parse_string_literal(value: &str) -> Option<String> {
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            let mut out = String::new();
            let mut escaped = false;
            for c in chars.by_ref() {
                if escaped {
                    out.push(c);
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    return Some(out);
                } else {
                    out.push(c);
                }
            }
            return None;
        }
    }
    None
}

fn find_calls<'a>(source: &'a str, name: &str) -> Vec<&'a str> {
    let mut calls = Vec::new();
    let mut cursor = 0;
    while let Some(idx) = source[cursor..].find(name) {
        let start = cursor + idx;
        let after_name = start + name.len();
        let rest = &source[after_name..];
        let open_paren = match rest.find('(') {
            Some(pos) => after_name + pos,
            None => break,
        };
        calls.push(&source[open_paren..]);
        cursor = open_paren + 1;
    }
    calls
}

fn parse_call_args(call_source: &str) -> Option<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut escaped = false;
    for ch in call_source.chars() {
        if in_string {
            current.push(ch);
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '(' => {
                depth += 1;
                if depth > 1 {
                    current.push(ch);
                }
            }
            ')' => {
                if depth == 1 {
                    if !current.trim().is_empty() {
                        args.push(current.trim().to_string());
                    }
                    return Some(args);
                }
                depth -= 1;
                current.push(ch);
            }
            '"' => {
                in_string = true;
                current.push(ch);
            }
            ',' if depth == 1 => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }
    None
}

fn extract_doc_comments(
    src_root: &Path,
) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut docs = BTreeMap::new();
    for entry in fs::read_dir(src_root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path)?;
        let mut doc_lines: Vec<String> = Vec::new();
        for line in source.lines() {
            let trimmed = line.trim_start();
            if let Some(rest) = trimmed.strip_prefix("///") {
                doc_lines.push(rest.trim().to_string());
                continue;
            }
            if trimmed.starts_with("#[") {
                continue;
            }
            if let Some(name) = extract_item_name(trimmed, "fn") {
                if !doc_lines.is_empty() {
                    docs.insert(name, doc_lines.join("\n"));
                }
                doc_lines.clear();
                continue;
            }
            if let Some(name) = extract_item_name(trimmed, "struct") {
                if !doc_lines.is_empty() {
                    docs.insert(name, doc_lines.join("\n"));
                }
                doc_lines.clear();
                continue;
            }
            if !trimmed.is_empty() {
                doc_lines.clear();
            }
        }
    }
    Ok(docs)
}

fn extract_item_name(line: &str, keyword: &str) -> Option<String> {
    let marker = format!("{keyword} ");
    let pos = line.find(&marker)?;
    let rest = &line[pos + marker.len()..];
    let mut name = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
        } else {
            break;
        }
    }
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn apply_doc_info(entry: &mut FunctionEntry, doc: &str) {
    let doc_info = parse_doc_comment(doc);
    if entry.signature.is_none() {
        entry.signature = doc_info.signature;
    }
    if entry.description.is_none() {
        entry.description = doc_info.description;
    }
    if entry.section.is_none() {
        entry.section = doc_info.section;
    }
}

fn parse_doc_comment(doc: &str) -> DocInfoParsed {
    let mut signature = None;
    let mut description = None;
    let mut section = None;
    let mut fallback_lines = Vec::new();

    for line in doc.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("SQL:") {
            signature = Some(rest.trim().to_string());
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("Usage:") {
            description = Some(rest.trim().to_string());
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("Section:") {
            section = parse_section(rest.trim());
            continue;
        }
        fallback_lines.push(trimmed.to_string());
    }

    if description.is_none() && !fallback_lines.is_empty() {
        description = Some(fallback_lines.join(" "));
    }

    DocInfoParsed {
        signature,
        description,
        section,
    }
}

fn parse_section(value: &str) -> Option<DocSection> {
    match value {
        "Durable" => Some(DocSection::Durable),
        "Schema" => Some(DocSection::Schema),
        "Meta" => Some(DocSection::Meta),
        _ => None,
    }
}

#[derive(Debug)]
struct DocInfoParsed {
    signature: Option<String>,
    description: Option<String>,
    section: Option<DocSection>,
}

fn render_markdown<'a>(entries: impl Iterator<Item = &'a FunctionEntry>) -> String {
    let mut sections: BTreeMap<DocSection, Vec<&FunctionEntry>> = BTreeMap::new();
    for entry in entries {
        let section = entry.section.unwrap_or(DocSection::Durable);
        sections.entry(section).or_default().push(entry);
    }
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str("title: SQLite Functions\n");
    out.push_str("---\n\n");
    out.push_str(
        "The [absurd-sqlite-extension][absurd-sqlite-extension] provides a collection of\n",
    );
    out.push_str(
        "SQLite functions to interact with the durable workflow system. The schema and base\n",
    );
    out.push_str("settings of the workflow system are managed by a few \"meta\" functions.\n\n");
    out.push_str("## Durable Workflow Functions\n\n");
    for entry in sections.get(&DocSection::Durable).into_iter().flatten() {
        out.push_str(&render_list_item(entry));
    }
    out.push_str("\n## Schema Management\n\n");
    for entry in sections.get(&DocSection::Schema).into_iter().flatten() {
        out.push_str(&render_list_item(entry));
    }
    out.push_str("\n## Meta Functions\n\n");
    for entry in sections.get(&DocSection::Meta).into_iter().flatten() {
        out.push_str(&render_list_item(entry));
    }
    out.push_str(
        "\n[absurd-sqlite-extension]: https://github.com/b4fun/absurd-sqlite/tree/main/absurd-sqlite-extension\n",
    );
    out.push_str(
        "\n{/* Generated by extract_sqlite_functions.rs, Run `cargo run -p absurd-sqlite-extension --bin extract_sqlite_functions -- --out docs-starlight/src/content/docs/reference/sqlite-functions.mdx` to regenerate. */}\n",
    );
    out
}

fn render_list_item(entry: &FunctionEntry) -> String {
    let mut line = String::new();
    let signature = entry.signature.as_deref().unwrap_or(entry.name.as_str());
    line.push_str("- `");
    line.push_str(signature);
    line.push('`');
    if let Some(desc) = entry.description.as_deref() {
        if !desc.is_empty() {
            line.push_str(": ");
            line.push_str(desc);
        }
    }
    line.push('\n');
    line
}
