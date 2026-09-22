// SQLite 索引存储
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

pub struct Db {
    pub conn: Connection,
}

impl Db {
    pub fn open(path: &Path) -> Result<Db, String> {
        let conn = Connection::open(path).map_err(|e| e.to_string())?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
             CREATE TABLE IF NOT EXISTS mails (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 path TEXT UNIQUE NOT NULL,
                 file_name TEXT NOT NULL,
                 size INTEGER NOT NULL,
                 mtime INTEGER NOT NULL,
                 indexed_at INTEGER NOT NULL,
                 subject TEXT DEFAULT '',
                 from_addr TEXT DEFAULT '',
                 to_addr TEXT DEFAULT '',
                 date TEXT DEFAULT '',
                 body_text TEXT DEFAULT '',
                 attachments TEXT DEFAULT ''
             );
             CREATE INDEX IF NOT EXISTS idx_subject ON mails(subject);
             ",
        )
        .map_err(|e| e.to_string())?;
        Ok(Db { conn })
    }

    pub fn get_meta(&self, key: &str) -> Option<String> {
        self.conn
            .query_row(
                "SELECT value FROM meta WHERE key=?1",
                params![key],
                |r| r.get(0),
            )
            .ok()
    }

    pub fn set_meta(&self, key: &str, value: &str) {
        let _ = self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        );
    }

    pub fn count(&self) -> i64 {
        self.conn
            .query_row("SELECT COUNT(*) FROM mails", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn upsert(&self, m: &IndexedMail) {
        let _ = self.conn.execute(
            "INSERT INTO mails(path,file_name,size,mtime,indexed_at,subject,from_addr,to_addr,date,body_text,attachments)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(path) DO UPDATE SET
               file_name=excluded.file_name, size=excluded.size, mtime=excluded.mtime,
               indexed_at=excluded.indexed_at, subject=excluded.subject,
               from_addr=excluded.from_addr, to_addr=excluded.to_addr,
               date=excluded.date, body_text=excluded.body_text, attachments=excluded.attachments",
            params![m.path, m.file_name, m.size, m.mtime, m.indexed_at,
                    m.subject, m.from_addr, m.to_addr, m.date, m.body_text, m.attachments],
        );
    }

    pub fn all_paths(&self) -> Vec<(i64, String)> {
        let mut stmt = match self.conn.prepare("SELECT id, path FROM mails") {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)));
        match rows {
            Ok(it) => it.filter_map(|x| x.ok()).collect(),
            Err(_) => vec![],
        }
    }

    pub fn delete_by_id(&self, id: i64) {
        let _ = self
            .conn
            .execute("DELETE FROM mails WHERE id=?1", params![id]);
    }

    /// 正则检索：在 文件名/主题/发件人/收件人/日期/附件（及正文）中用正则匹配
    pub fn search_regex(&self, patterns: &[String], scope_body: bool) -> Result<Vec<SearchHit>, String> {
        let mut regexes = Vec::new();
        for p in patterns {
            let re = regex::RegexBuilder::new(p)
                .case_insensitive(true)
                .build()
                .map_err(|e| format!("正则表达式无效 \"{}\": {}", p, e))?;
            regexes.push(re);
        }
        let mut stmt = self
            .conn
            .prepare("SELECT id, path, file_name, subject, from_addr, to_addr, date, attachments, body_text FROM mails ORDER BY id LIMIT 200000")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok(SearchHit {
                    id: r.get(0)?,
                    path: r.get(1)?,
                    file_name: r.get(2)?,
                    subject: r.get(3)?,
                    from_addr: r.get(4)?,
                    to_addr: r.get(5)?,
                    date: r.get(6)?,
                    attachments: r.get(7)?,
                    score: 0,
                })
            })
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in rows.filter_map(|x| x.ok()) {
            let mut matched = false;
            for re in &regexes {
                let hit = re.is_match(&row.file_name)
                    || re.is_match(&row.subject)
                    || re.is_match(&row.from_addr)
                    || re.is_match(&row.to_addr)
                    || re.is_match(&row.date)
                    || re.is_match(&row.attachments);
                let hit = if hit || !scope_body {
                    hit
                } else {
                    // 正文不在 SearchHit 里，需要单独取
                    self.conn
                        .query_row(
                            "SELECT body_text FROM mails WHERE id=?1",
                            params![row.id],
                            |r| r.get::<_, String>(0),
                        )
                        .map(|body| re.is_match(&body))
                        .unwrap_or(false)
                };
                if hit {
                    matched = true;
                    break;
                }
            }
            if matched {
                out.push(row.clone());
                if out.len() >= 20000 {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub fn get_body(&self, id: i64) -> Option<String> {
        self.conn
            .query_row(
                "SELECT body_text FROM mails WHERE id=?1",
                params![id],
                |r| r.get(0),
            )
            .ok()
    }

    /// 关键词检索：在 文件名/主题/发件人/收件人/日期/正文中查找（大小写不敏感）
    pub fn search(&self, keywords: &[String], scope_body: bool) -> Result<Vec<SearchHit>, String> {
        let mut sql = String::from(
            "SELECT id, path, file_name, subject, from_addr, to_addr, date, attachments FROM mails WHERE ",
        );
        let mut conds = Vec::new();
        let mut params_v: Vec<String> = Vec::new();
        for (i, kw) in keywords.iter().enumerate() {
            let p = format!("%{}%", kw.to_lowercase());
            let mut c = String::new();
            c.push_str("(LOWER(file_name) LIKE ? OR LOWER(subject) LIKE ? OR LOWER(from_addr) LIKE ? OR LOWER(to_addr) LIKE ? OR LOWER(date) LIKE ? OR LOWER(attachments) LIKE ?)");
            if scope_body {
                c.push_str(" OR LOWER(body_text) LIKE ?");
                params_v.push(p.clone()); // body
            }
            // 为占位符填充
            // sqlite 允许 ? 顺序编号，但这里统一用 ?N 编号更安全
            let base = params_v.len() + 1;
            let _ = base;
            conds.push(c);
            // 6 个头部字段 + 可能 1 个正文字段
            let n_header = 6;
            for _ in 0..n_header {
                params_v.push(p.clone());
            }
            if scope_body {
                // 已在上面 push 过一次 body 的参数，但顺序需与 SQL 一致，重排：
                // 实际上 SQL 中 body LIKE 在最后一个 ?，参数顺序需匹配。
            }
        }
        sql.push_str(&conds.join(" OR "));
        sql.push_str(" ORDER BY id LIMIT 20000");

        // 由于参数顺序问题，改用位置参数 ?N 重写（更清晰可靠）
        let mut sql2 = String::from(
            "SELECT id, path, file_name, subject, from_addr, to_addr, date, attachments FROM mails WHERE (",
        );
        let mut params_n: Vec<String> = Vec::new();
        let or_conds: Vec<String> = keywords
            .iter()
            .map(|kw| {
                let mut c = String::from("(");
                let mut first = true;
                let fields = ["file_name","subject","from_addr","to_addr","date","attachments"];
                for f in fields.iter() {
                    if !first { c.push_str(" OR "); }
                    first = false;
                    let n = params_n.len() + 1;
                    c.push_str(&format!("LOWER({}) LIKE ?{}", f, n));
                    params_n.push(format!("%{}%", kw.to_lowercase()));
                }
                if scope_body {
                    let n = params_n.len() + 1;
                    c.push_str(&format!(" OR LOWER(body_text) LIKE ?{}", n));
                    params_n.push(format!("%{}%", kw.to_lowercase()));
                }
                c.push(')');
                c
            })
            .collect();
        sql2.push_str(&or_conds.join(" OR "));
        sql2.push_str(") ORDER BY id LIMIT 20000");

        let _ = sql; // 弃用第一版
        let mut stmt = self.conn.prepare(&sql2).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_n.iter()), |r| {
                Ok(SearchHit {
                    id: r.get(0)?,
                    path: r.get(1)?,
                    file_name: r.get(2)?,
                    subject: r.get(3)?,
                    from_addr: r.get(4)?,
                    to_addr: r.get(5)?,
                    date: r.get(6)?,
                    attachments: r.get(7)?,
                    score: 0,
                })
            })
            .map_err(|e| e.to_string())?;
        Ok(rows.filter_map(|x| x.ok()).collect())
    }
}

pub struct IndexedMail {
    pub path: String,
    pub file_name: String,
    pub size: u64,
    pub mtime: i64,
    pub indexed_at: i64,
    pub subject: String,
    pub from_addr: String,
    pub to_addr: String,
    pub date: String,
    pub body_text: String,
    pub attachments: String,
}

#[derive(Clone)]
pub struct SearchHit {
    pub id: i64,
    pub path: String,
    pub file_name: String,
    pub subject: String,
    pub from_addr: String,
    pub to_addr: String,
    pub date: String,
    pub attachments: String,
    pub score: usize, // 关键词出现次数（词频）
}

pub fn default_db_path() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("mail_index.db")
}
