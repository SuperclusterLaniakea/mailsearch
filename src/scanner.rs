// 扫描+索引模块：遍历目录（含子目录），按扩展名过滤，多线程解析邮件并写入 SQLite
use crate::db::{Db, IndexedMail};
use crate::mail_parse::parse_file;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

pub struct ScanOptions {
    pub root: PathBuf,
    /// 允许的扩展名（小写、不含点）。空 = 不过滤扩展名
    pub include_exts: Vec<String>,
    /// 是否把不在扩展名列表中的文件排除（false = 所有文件都尝试解析）
    pub exclude_by_ext: bool,
    /// 增量：文件未变化则跳过
    pub incremental: bool,
}

pub struct ScanProgress {
    pub scanned: usize,
    pub indexed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub done: bool,
    pub cancelled: bool,
}

pub struct ScanHandle {
    pub cancel: Arc<AtomicBool>,
}

/// 收集所有候选文件（按扩展名过滤）
pub fn collect_files(root: &Path, opts: &ScanOptions) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let walker = walkdir::WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok());
    for entry in walker {
        if !entry.file_type().is_file() {
            continue;
        }
        if !opts.include_exts.is_empty() {
            let ext = entry
                .path()
                .extension()
                .map(|e| e.to_string_lossy().to_lowercase())
                .unwrap_or_default();
            let listed = opts.include_exts.iter().any(|x| *x == ext);
            // exclude_by_ext=true 时只收列表内扩展名；false 时列表内收、列表外也收
            if opts.exclude_by_ext && !listed {
                continue;
            }
        }
        out.push(entry.path().to_path_buf());
    }
    out.sort();
    out
}

/// 执行扫描索引（阻塞；通过 log_tx 发送日志行）。返回是否正常完成（未取消）
pub fn run_scan(
    db_path: &Path,
    opts: ScanOptions,
    log_tx: Sender<String>,
    cancel: Arc<AtomicBool>,
) -> bool {
    let log = |msg: String| {
        let _ = log_tx.send(msg);
    };

    log(format!("打开索引库: {}", db_path.display()));
    let db = match Db::open(db_path) {
        Ok(d) => d,
        Err(e) => {
            log(format!("[错误] 打开索引库失败: {}", e));
            return false;
        }
    };

    log(format!("扫描目录: {}", opts.root.display()));
    let files = collect_files(&opts.root, &opts);
    let total = files.len();
    log(format!(
        "发现 {} 个候选文件（扩展名过滤: {}）",
        total,
        if opts.include_exts.is_empty() {
            "无（全部文件）".to_string()
        } else {
            format!(
                "{} {}",
                opts.include_exts.join(","),
                if opts.exclude_by_ext { "（排除列表外）" } else { "（列表外也尝试解析）" }
            )
        }
    ));

    let db = Arc::new(db);
    let scanned = Arc::new(AtomicUsize::new(0));
    let indexed = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let log_shared: Arc<Mutex<Sender<String>>> = Arc::new(Mutex::new(log_tx.clone()));

    // 已索引路径 mtime 表（增量用）
    let known: std::collections::HashMap<String, i64> = if opts.incremental {
        db.all_paths()
            .into_iter()
            .filter_map(|(id, p)| {
                let mtime = std::fs::metadata(&p)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(-1);
                Some((p, mtime))
            })
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    let known = Arc::new(known);

    let results: Vec<IndexedMail> = files
        .par_iter()
        .filter_map(|path| {
            if cancel.load(Ordering::Relaxed) {
                return None;
            }
            let i = scanned.fetch_add(1, Ordering::Relaxed) + 1;
            if i % 200 == 0 || i == total {
                let lg = log_shared.lock().unwrap().clone();
                let _ = lg.send(format!("进度 {}/{}", i, total));
            }

            let meta = match std::fs::metadata(path) {
                Ok(m) => m,
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            };
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            let path_str = path.to_string_lossy().to_string();
            if opts.incremental {
                if let Some(old_mtime) = known.get(&path_str) {
                    if *old_mtime == mtime {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                }
            }

            match parse_file(path) {
                Ok(pm) => {
                    indexed.fetch_add(1, Ordering::Relaxed);
                    Some(IndexedMail {
                        path: path_str,
                        file_name: path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_default(),
                        size,
                        mtime,
                        indexed_at: chrono::Utc::now().timestamp(),
                        subject: pm.subject,
                        from_addr: pm.from,
                        to_addr: pm.to,
                        date: pm.date,
                        body_text: truncate(&pm.body_text, 500_000),
                        attachments: pm.attachments.join(", "),
                    })
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    if failed.load(Ordering::Relaxed) <= 20 {
                        let lg = log_shared.lock().unwrap().clone();
                        let _ = lg.send(format!("[跳过] {}: {}", path_str, e));
                    }
                    None
                }
            }
        })
        .collect();

    if cancel.load(Ordering::Relaxed) {
        log("[已取消]".to_string());
        // 仍写入已解析的部分
    }

    log(format!("写入数据库 {} 条…", results.len()));
    for m in &results {
        db.upsert(m);
    }

    // 增量模式下清理已消失的文件
    if opts.incremental && !cancel.load(Ordering::Relaxed) {
        let mut removed = 0usize;
        for (id, p) in db.all_paths() {
            if !Path::new(&p).exists() {
                db.delete_by_id(id);
                removed += 1;
            }
        }
        if removed > 0 {
            log(format!("清理已删除文件 {} 条", removed));
        }
    }

    log(format!(
        "完成：扫描 {}，新索引 {}，跳过(未变) {}，失败 {}。库内共 {} 条。",
        scanned.load(Ordering::Relaxed),
        indexed.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        db.count()
    ));
    !cancel.load(Ordering::Relaxed)
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}
