// 邮件检索工具 - egui UI
mod db;
mod mail_parse;
mod scanner;

use db::{Db, SearchHit, default_db_path};
use eframe::egui;
use egui::{Color32, RichText, ScrollArea};
use mail_parse::parse_file;
use scanner::{ScanOptions, ScanHandle};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

// ---------- 读取邮件视图（后台线程解析，避免 UI 卡顿） ----------
#[derive(Default)]
struct MailViewer {
    open: bool,
    loading: bool,
    path: String,
    // 缓存: path -> (subject, header_text, body_text)
    cache: Arc<Mutex<std::collections::HashMap<String, (String, String, String)>>>,
    rx: Option<Receiver<String>>, // 完成/错误信号
}

impl MailViewer {
    fn load(&mut self, path: &str) {
        self.open = true;
        self.loading = true;
        self.path = path.to_string();
        if self.cache.lock().unwrap().contains_key(path) {
            self.loading = false;
            return;
        }
        let path2 = path.to_string();
        let cache = self.cache.clone();
        let (tx, rx) = channel::<String>();
        self.rx = Some(rx);
        std::thread::spawn(move || {
            let result = (|| -> Result<(String, String, String), String> {
                let pm = parse_file(std::path::Path::new(&path2))?;
                let header = format!(
                    "主题: {}\n发件人: {}\n收件人: {}\n日期: {}\n附件: {}\n{}",
                    if pm.subject.is_empty() {"(无主题)"} else {&pm.subject},
                    pm.from, pm.to, pm.date,
                    if pm.attachments.is_empty() {"(无)".to_string()} else {pm.attachments.join(", ")},
                    "-".repeat(60)
                );
                let body = if !pm.body_text.is_empty() {
                    pm.body_text
                } else if !pm.body_html.is_empty() {
                    // 粗略去 HTML 标签便于查看
                    let re = regex::Regex::new(r"(?is)<(script|style)[^>]*>.*?</\1>|<[^>]+>").unwrap();
                    let s = re.replace_all(&pm.body_html, " ");
                    let s = html_entities(&s);
                    s
                } else {
                    "(无文本正文)".to_string()
                };
                Ok((pm.subject, header, body))
            })();
            match result {
                Ok(v) => { cache.lock().unwrap().insert(path2.clone(), v); let _ = tx.send(path2); }
                Err(e) => { cache.lock().unwrap().insert(path2.clone(), ("[解析失败]".into(), String::new(), e)); let _ = tx.send(path2); }
            }
        });
    }
}

fn html_entities(s: &str) -> String {
    s.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

// ---------- 应用 ----------
#[derive(PartialEq)]
enum ScanState { Idle, Running, Done(bool) }

#[derive(PartialEq, Clone, Copy)]
enum SortMode {
    TimeDesc,   // 时间 新→旧
    TimeAsc,    // 时间 旧→新
    ScoreDesc,  // 词频 高→低
    ScoreAsc,   // 词频 低→高
    Subject,    // 主题 A→Z
}

impl SortMode {
    fn label(&self) -> &'static str {
        match self {
            SortMode::TimeDesc => "时间 新→旧",
            SortMode::TimeAsc => "时间 旧→新",
            SortMode::ScoreDesc => "词频 高→低",
            SortMode::ScoreAsc => "词频 低→高",
            SortMode::Subject => "主题 A→Z",
        }
    }
    fn all() -> [SortMode; 5] {
        [SortMode::TimeDesc, SortMode::TimeAsc, SortMode::ScoreDesc, SortMode::ScoreAsc, SortMode::Subject]
    }
    fn sort_hits(hits: &mut [db::SearchHit], mode: SortMode) {
        match mode {
            SortMode::TimeDesc => hits.sort_by(|a, b| b.date.cmp(&a.date).then(a.subject.cmp(&b.subject))),
            SortMode::TimeAsc => hits.sort_by(|a, b| a.date.cmp(&b.date).then(a.subject.cmp(&b.subject))),
            SortMode::ScoreDesc => hits.sort_by(|a, b| b.score.cmp(&a.score).then(b.date.cmp(&a.date))),
            SortMode::ScoreAsc => hits.sort_by(|a, b| a.score.cmp(&b.score).then(b.date.cmp(&a.date))),
            SortMode::Subject => hits.sort_by(|a, b| a.subject.to_lowercase().cmp(&b.subject.to_lowercase())),
        }
    }
}

struct App {
    root_dir: String,
    ext_include: String,     // 逗号分隔扩展名, 如 "eml,msg"
    exclude_by_ext: bool,    // true: 只检索列表内扩展名; false: 列表外文件也尝试解析
    scope_body: bool,        // 检索范围含正文
    use_regex: bool,         // 关键词按正则匹配
    sort_mode: SortMode,     // 结果排序方式
    keywords: String,
    use_last_db: bool,       // 直接使用上次索引库（增量）
    db_path: PathBuf,

    log_lines: Arc<Mutex<Vec<String>>>,
    log_rx: Option<Receiver<String>>,
    scan_state: ScanState,
    scan_handle: Option<JoinHandle<bool>>,
    cancel_flag: Option<Arc<AtomicBool>>,
    scan_progress: Arc<(Mutex<usize>, Mutex<usize>)>, // (done,total)

    hits: Vec<SearchHit>,
    selected: Option<usize>,
    viewer: MailViewer,

    db_count: i64,
    last_db_dir: Option<String>,
}

impl Default for App {
    fn default() -> Self {
        let db_path = default_db_path();
        let (db_count, last_dir) = Db::open(&db_path).map_or((0, None), |db| {
            (db.count(), db.get_meta("last_root"))
        });
        App {
            root_dir: last_dir.clone().unwrap_or_default(),
            ext_include: "eml".to_string(),
            exclude_by_ext: true,
            scope_body: true,
            use_regex: false,
            sort_mode: SortMode::TimeDesc,
            keywords: String::new(),
            use_last_db: true,
            db_path,
            log_lines: Arc::new(Mutex::new(Vec::new())),
            log_rx: None,
            scan_state: ScanState::Idle,
            scan_handle: None,
            cancel_flag: None,
            scan_progress: Arc::new((Mutex::new(0), Mutex::new(0))),
            hits: vec![],
            selected: None,
            viewer: MailViewer::default(),
            db_count,
            last_db_dir: last_dir,
        }
    }
}

impl App {
    fn log(&self, s: impl AsRef<str>) {
        let mut lg = self.log_lines.lock().unwrap();
        lg.push(s.as_ref().to_string());
        if lg.len() > 5000 { lg.drain(0..2000); }
    }

    fn start_scan(&mut self) {
        if self.root_dir.trim().is_empty() {
            self.log("[提示] 请先选择目录");
            return;
        }
        let root = PathBuf::from(self.root_dir.trim());
        if !root.is_dir() {
            self.log("[错误] 目录不存在");
            return;
        }
        let exts: Vec<String> = self
            .ext_include
            .split(|c| c == ',' || c == ';' || c == ' ')
            .map(|s| s.trim().trim_start_matches('.').to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let opts = ScanOptions {
            root: root.clone(),
            include_exts: exts.clone(),
            exclude_by_ext: self.exclude_by_ext,
            incremental: self.use_last_db,
        };
        let (tx, rx) = channel::<String>();
        self.log_rx = Some(rx);
        self.log_lines.lock().unwrap().clear();
        self.log(format!("=== 开始扫描 {} ===", root.display()));
        if !exts.is_empty() {
            self.log(format!("扩展名过滤: [{}]，排除列表外文件: {}", exts.join(", "), if self.exclude_by_ext {"是"} else {"否"}));
        } else {
            self.log("扩展名过滤: 未设置（所有文件都尝试解析）".to_string());
        }

        let cancel = Arc::new(AtomicBool::new(false));
        self.cancel_flag = Some(cancel.clone());
        self.scan_state = ScanState::Running;
        let db_path = self.db_path.clone();
        let prog = self.scan_progress.clone();
        let log_arc = self.log_lines.clone();
        let root_str = root.to_string_lossy().to_string();

        *prog.0.lock().unwrap() = 0;
        *prog.1.lock().unwrap() = 0;

        let handle = std::thread::spawn(move || {
            // 桥接日志线程：扫描线程 -> 进度轮询 -> UI
            let prog2 = prog.clone();
            let log2 = log_arc.clone();
            let (ptx, prx) = channel::<String>();
            let h2 = std::thread::spawn(move || {
                for line in prx {
                    if let Some(rest) = line.strip_prefix("进度 ") {
                        if let Some((a, b)) = rest.split_once('/') {
                            if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                                *prog2.0.lock().unwrap() = a;
                                *prog2.1.lock().unwrap() = b;
                            }
                        }
                    }
                    let mut lg = log2.lock().unwrap();
                    lg.push(line);
                    if lg.len() > 5000 { lg.drain(0..2000); }
                }
            });
            let ok = scanner::run_scan(&db_path, opts, ptx, cancel);
            let _ = h2.join();
            // 记录 last_root
            if let Ok(db) = Db::open(&db_path) {
                db.set_meta("last_root", &root_str);
            }
            ok
        });
        self.scan_handle = Some(handle);
    }

    fn do_search(&mut self) {
        let kws: Vec<String> = self
            .keywords
            .split(|c: char| c == ',' || c == ';' || c == '　')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if kws.is_empty() {
            self.log("[提示] 请输入关键词（多个用逗号或空格分隔）");
            return;
        }
        let db = match Db::open(&self.db_path) {
            Ok(d) => d,
            Err(e) => { self.log(format!("[错误] 打开索引库失败: {}", e)); return; }
        };
        let result = if self.use_regex {
            db.search_regex(&kws, self.scope_body)
        } else {
            db.search(&kws, self.scope_body)
        };
        match result {
            Ok(mut hits) => {
                // 词频统计：各关键词在 正文+主题+发件人+收件人+文件名 中出现的总次数
                let kws_lower: Vec<String> = kws.iter().map(|k| k.to_lowercase()).collect();
                for h in hits.iter_mut() {
                    let mut texts: Vec<String> = vec![
                        h.subject.clone(),
                        h.from_addr.clone(),
                        h.to_addr.clone(),
                        h.file_name.clone(),
                    ];
                    if self.scope_body {
                        if let Some(body) = db.get_body(h.id) {
                            texts.push(body);
                        }
                    }
                    let text = texts.join("\n").to_lowercase();
                    for kw in &kws_lower {
                        if kw.is_empty() { continue; }
                        h.score += text.matches(kw.as_str()).count();
                    }
                }
                self.log(format!(
                    "检索到 {} 条结果（{}: {}）",
                    hits.len(),
                    if self.use_regex { "正则" } else { "关键词" },
                    kws.join(", ")
                ));
                // 排序交给结果面板的排序选择器，这里仅保持库序
                self.hits = hits;
                self.selected = None;
            }
            Err(e) => self.log(format!("[错误] 检索失败: {}", e)),
        }
        self.db_count = db.count();
    }

    fn poll_scan(&mut self) {
        if let Some(rx) = &self.log_rx {
            let mut got = false;
            while let Ok(line) = rx.try_recv() {
                let mut lg = self.log_lines.lock().unwrap();
                lg.push(line);
                if lg.len() > 5000 { lg.drain(0..2000); }
                got = true;
            }
            let _ = got;
        }
        if self.scan_state == ScanState::Running {
            if let Some(h) = &self.scan_handle {
                if h.is_finished() {
                    let ok = self.scan_handle.take().map(|h| h.join().unwrap_or(false)).unwrap_or(false);
                    self.scan_state = ScanState::Done(ok);
                    self.log(if ok {"=== 扫描完成 ==="} else {"=== 扫描已取消/结束 ==="});
                    if let Ok(db) = Db::open(&self.db_path) { self.db_count = db.count(); }
                }
            }
        }
        // 后台邮件解析完成信号
        if let Some(rx) = self.viewer.rx.take() {
            match rx.try_recv() {
                Ok(p) if p == self.viewer.path => self.viewer.loading = false,
                Ok(_) => { self.viewer.rx = Some(rx); } // 旧任务，忽略
                Err(_) => { self.viewer.rx = Some(rx); }
            }
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_scan();
        // 扫描中保持重绘
        if self.scan_state == ScanState::Running || self.viewer.loading {
            ctx.request_repaint();
        }

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(4.0);
            egui::Grid::new("ctl").num_columns(4).spacing([8.0, 6.0]).show(ui, |ui| {
                ui.label("目录:");
                ui.add_sized([ui.available_width() - 140.0, 20.0], egui::TextEdit::singleline(&mut self.root_dir));
                if ui.button("浏览...").clicked() {
                    if let Some(p) = rfd::FileDialog::new().pick_folder() {
                        self.root_dir = p.to_string_lossy().to_string();
                    }
                }
                ui.end_row();

                ui.label("扩展名:");
                ui.horizontal(|ui| {
                    ui.add_sized([200.0, 20.0], egui::TextEdit::singleline(&mut self.ext_include))
                        .on_hover_text("逗号分隔，如: eml 或 eml,msg；留空 = 不过滤");
                    ui.checkbox(&mut self.exclude_by_ext, "排除列表外文件")
                        .on_hover_text("勾选: 只检索列表内扩展名的文件\n不勾选: 列表内优先，列表外的无后缀文件也尝试解析");
                });
                ui.end_row();

                ui.label("关键词:");
                ui.horizontal(|ui| {
                    ui.add_sized([ui.available_width() - 220.0, 20.0], egui::TextEdit::singleline(&mut self.keywords))
                        .on_hover_text("多个关键词用逗号分隔，命中任一即显示");
                    if ui.add_enabled(self.scan_state != ScanState::Running, egui::Button::new("🔍 检索")).clicked() {
                        self.do_search();
                    }
                    ui.checkbox(&mut self.scope_body, "含正文");
                    ui.checkbox(&mut self.use_regex, "正则")
                        .on_hover_text("勾选后关键词按正则表达式匹配（多个正则用逗号分隔，命中任一即显示）\n例: 空客|AIRBUS  \\d{4}-\\d{2}-\\d{2}");
                });
                ui.end_row();

                ui.label("索引库:");
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.use_last_db, "增量(利用上次索引库)");
                    ui.label(format!("库内 {} 封", self.db_count));
                    ui.label(RichText::new(self.db_path.display().to_string()).weak().small());
                });
                ui.end_row();
            });

            ui.horizontal(|ui| {
                let running = self.scan_state == ScanState::Running;
                if ui.add_enabled(!running, egui::Button::new("▶ 开始/更新索引")).clicked() {
                    self.start_scan();
                }
                if ui.add_enabled(running, egui::Button::new("■ 取消")).clicked() {
                    if let Some(c) = &self.cancel_flag { c.store(true, Ordering::Relaxed); }
                }
                if running {
                    let (d, t) = (*self.scan_progress.0.lock().unwrap(), *self.scan_progress.1.lock().unwrap());
                    let frac = if t > 0 { d as f32 / t as f32 } else { 0.0 };
                    ui.add(egui::ProgressBar::new(frac).show_percentage().desired_width(300.0));
                    ui.label(format!("{}/{}", d, t));
                }
            });
            ui.add_space(4.0);
        });

        // 中间：左结果列表
        egui::SidePanel::left("results").resizable(true).default_width(520.0).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(format!("检索结果 ({} 条)", self.hits.len()));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label("排序:");
                    let before = self.sort_mode;
                    egui::ComboBox::from_id_source("sort_mode")
                        .width(110.0)
                        .selected_text(self.sort_mode.label())
                        .show_ui(ui, |ui| {
                            for m in SortMode::all() {
                                ui.selectable_value(&mut self.sort_mode, m, m.label());
                            }
                        });
                    if before != self.sort_mode {
                        SortMode::sort_hits(&mut self.hits, self.sort_mode);
                    }
                });
            });
            ui.separator();
            ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                for (i, h) in self.hits.iter().enumerate() {
                    let sel = self.selected == Some(i);
                    let label_text = format!("{}  |  {}  |  {}", &h.date, &h.from_addr, &h.subject);
                    let resp = ui.selectable_label(sel, label_text);
                    if resp.clicked() {
                        self.selected = Some(i);
                        self.viewer.load(&h.path.clone());
                    }
                    if resp.double_clicked() {
                        // 双击：用 Outlook（系统默认邮件程序）打开 eml
                        let _ = open_file::open_with_mail_client(&h.path);
                        self.log(format!("已用 Outlook/默认邮件程序打开: {}", h.file_name));
                    }
                    resp.context_menu(|ui| {
                        if ui.button("打开文件位置").clicked() {
                            if let Some(dir) = std::path::Path::new(&h.path).parent() {
                                let _ = open_file::reveal_in_explorer(dir);
                            }
                            ui.close_menu();
                        }
                        if ui.button("用 Outlook 打开").clicked() {
                            let _ = open_file::open_with_mail_client(&h.path);
                            ui.close_menu();
                        }
                        if ui.button("用系统默认程序打开").clicked() {
                            let _ = open_file::open_with_system(&h.path);
                            ui.close_menu();
                        }
                    });
                }
                if self.hits.is_empty() {
                    ui.weak("（无结果。请先建立索引，再输入关键词检索）");
                }
            });
        });

        // 底部：进展日志（页面最底端）
        egui::TopBottomPanel::bottom("log_panel")
            .resizable(true)
            .default_height(180.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("进展日志");
                    if ui.small_button("清空").clicked() {
                        self.log_lines.lock().unwrap().clear();
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.small_button("打开邮件所在文件夹").clicked() {
                            if let Some(i) = self.selected {
                                let p = PathBuf::from(&self.hits[i].path);
                                if let Some(dir) = p.parent() { let _ = open_file::open_or_reveal(dir); }
                            }
                        }
                    });
                });
                ui.separator();
                ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false, false]).show(ui, |ui| {
                    let lines = self.log_lines.lock().unwrap().clone();
                    for line in &lines {
                        let color = if line.starts_with("[错误") { Color32::RED }
                            else if line.starts_with("[跳过") { Color32::YELLOW }
                            else if line.starts_with("===") || line.starts_with("完成") { Color32::LIGHT_GREEN }
                            else { Color32::LIGHT_GRAY };
                        ui.label(RichText::new(line).color(color).monospace().size(12.0));
                    }
                });
            });

        // 中央：所选邮件全文
        egui::CentralPanel::default().show(ctx, |ui| {
            match self.selected {
                Some(i) if i < self.hits.len() => {
                    let hit = &self.hits[i];
                    ui.horizontal(|ui| {
                        ui.heading(if hit.subject.is_empty() { "(无主题)" } else { hit.subject.as_str() });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.small_button("用系统默认程序打开原始文件").clicked() {
                                let _ = open_file::open_with_system(&hit.path);
                            }
                        });
                    });
                    ui.separator();
                    if self.viewer.loading && self.viewer.path == hit.path {
                        ui.spinner();
                        ui.label("正在解析邮件…");
                    } else if let Some((subject, header, body)) = self.viewer.cache.lock().unwrap().get(&hit.path).cloned() {
                        ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                            ui.label(RichText::new(&header).weak().monospace());
                            ui.separator();
                            ui.label(RichText::new(&body).monospace());
                        });
                    } else {
                        ui.weak("（点击左侧条目查看邮件内容）");
                    }
                }
                _ => {
                    ui.weak("（检索后点击左侧条目，此处显示邮件全文）");
                }
            }
        });
    }
}

// 简单的文件打开辅助（避免引入 open crate 依赖）
mod open_file {
    use std::path::Path;
    pub fn open_with_system(p: &str) -> std::io::Result<()> {
        #[cfg(target_os = "windows")]
        { std::process::Command::new("cmd").args(["/C", "start", "", p]).spawn()?; }
        #[cfg(target_os = "macos")]
        { std::process::Command::new("open").arg(p).spawn()?; }
        #[cfg(all(unix, not(target_os = "macos")))]
        { std::process::Command::new("xdg-open").arg(p).spawn()?; }
        Ok(())
    }
    pub fn open_or_reveal(dir: &Path) -> std::io::Result<()> {
        open_with_system(&dir.to_string_lossy())
    }

    /// 用 Outlook（系统默认邮件客户端）打开 .eml 文件
    pub fn open_with_mail_client(p: &str) -> std::io::Result<()> {
        #[cfg(target_os = "windows")]
        {
            // 通过 ShellExecute "open" 走系统 .eml 文件关联（通常为 Outlook）
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            std::process::Command::new("cmd")
                .args(["/C", "start", "", p])
                .creation_flags(CREATE_NO_WINDOW)
                .spawn()?;
        }
        #[cfg(target_os = "macos")]
        { std::process::Command::new("open").arg(p).spawn()?; }
        #[cfg(all(unix, not(target_os = "macos")))]
        { std::process::Command::new("xdg-open").arg(p).spawn()?; }
        Ok(())
    }

    /// 在资源管理器中打开文件夹（定位文件位置）
    pub fn reveal_in_explorer(dir: &Path) -> std::io::Result<()> {
        #[cfg(target_os = "windows")]
        { std::process::Command::new("explorer").arg(dir).spawn()?; }
        #[cfg(target_os = "macos")]
        { std::process::Command::new("open").arg(dir).spawn()?; }
        #[cfg(all(unix, not(target_os = "macos")))]
        { std::process::Command::new("xdg-open").arg(dir).spawn()?; }
        Ok(())
    }
}

fn main() -> Result<(), eframe::Error> {
    // 命令行自检: mail-search --test-parse <file.eml>
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 3 && args[1] == "--test-parse" {
        match mail_parse::parse_file(std::path::Path::new(&args[2])) {
            Ok(pm) => {
                println!("主题: {}", pm.subject);
                println!("发件人: {}", pm.from);
                println!("收件人: {}", pm.to);
                println!("日期: {}", pm.date);
                println!("附件: {}", if pm.attachments.is_empty() {"(无)".into()} else {pm.attachments.join(", ")});
                println!("正文前200字: {}", pm.body_text.chars().take(200).collect::<String>());
                return Ok(());
            }
            Err(e) => { eprintln!("解析失败: {}", e); std::process::exit(1); }
        }
    }
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("邮件检索工具 (Mail Search)")
            .with_inner_size([1280.0, 800.0]),
        ..Default::default()
    };
    eframe::run_native(
        "邮件检索工具",
        options,
        Box::new(|cc| {
            // 注册系统中文字体，解决界面中文乱码
            let mut fonts = egui::FontDefinitions::default();
            let candidates = [
                "C:/Windows/Fonts/msyh.ttc",      // 微软雅黑
                "C:/Windows/Fonts/msyh.ttf",
                "C:/Windows/Fonts/simhei.ttf",    // 黑体
                "C:/Windows/Fonts/simsun.ttc",    // 宋体
            ];
            for (i, path) in candidates.iter().enumerate() {
                if let Ok(data) = std::fs::read(path) {
                    fonts.font_data.insert(
                        format!("cjk_{}", i),
                        egui::FontData::from_owned(data).into(),
                    );
                    // 插到默认字体族最前，优先使用
                    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
                        if let Some(list) = fonts.families.get_mut(&family) {
                            list.insert(0, format!("cjk_{}", i));
                        }
                    }
                    break;
                }
            }
            cc.egui_ctx.set_fonts(fonts);
            Box::new(App::default())
        }),
    )
}
