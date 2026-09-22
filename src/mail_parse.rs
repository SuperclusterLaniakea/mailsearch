// 邮件解析模块：基于 mail-parser 0.9 提取主题/发件人/收件人/日期/正文
use mail_parser::{Addr, MessageParser};
use std::path::Path;

pub struct ParsedMail {
    pub subject: String,
    pub from: String,
    pub to: String,
    pub date: String,
    pub body_text: String,
    pub body_html: String,
    pub attachments: Vec<String>,
}

fn addr_to_string(a: &Addr) -> String {
    let disp = a.name().unwrap_or("");
    let addr = a.address().unwrap_or("");
    if disp.is_empty() {
        addr.to_string()
    } else {
        format!("{} <{}>", disp, addr)
    }
}

fn addr_list(h: Option<&mail_parser::Address>) -> String {
    match h {
        Some(a) => a
            .iter()
            .map(addr_to_string)
            .collect::<Vec<_>>()
            .join("; "),
        None => String::new(),
    }
}

/// 从部件的 Content-Disposition/Content-Type 头中取文件名
fn part_filename(part: &mail_parser::MessagePart) -> String {
    for h in &part.headers {
        let name = h.name().to_lowercase();
        if name != "content-disposition" && name != "content-type" {
            continue;
        }
        if let mail_parser::HeaderValue::ContentType(ct) = &h.value {
            if let Some(a) = &ct.attribute("name") {
                return a.to_string();
            }
            if let Some(a) = &ct.attribute("filename") {
                return a.to_string();
            }
        }
    }
    String::new()
}

pub fn parse_file(path: &Path) -> Result<ParsedMail, String> {
    // 读入原始字节；UTF-8 失败时按 GBK 再转（老中文邮件常见）
    let raw = std::fs::read(path).map_err(|e| e.to_string())?;
    let bytes = if std::str::from_utf8(&raw).is_ok() {
        raw
    } else {
        let (decoded, _, _) = encoding_rs::GBK.decode(&raw);
        decoded.into_owned().into_bytes()
    };

    let msg = MessageParser::default()
        .parse(&bytes)
        .ok_or_else(|| "无法解析为邮件".to_string())?;

    let subject = msg.subject().unwrap_or("").to_string();
    let from = addr_list(msg.from());
    let to = addr_list(msg.to());
    let date = msg
        .date()
        .map(|d| {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                d.year, d.month, d.day, d.hour, d.minute, d.second
            )
        })
        .unwrap_or_default();

    let mut attachments = Vec::new();
    for part in msg.attachments() {
        let name = part_filename(part);
        if !name.is_empty() {
            attachments.push(name);
        }
    }

    let body_text = msg
        .body_text(0)
        .map(|c| c.into_owned())
        .unwrap_or_default();
    let body_html = msg
        .body_html(0)
        .map(|c| c.into_owned())
        .unwrap_or_default();

    Ok(ParsedMail {
        subject,
        from,
        to,
        date,
        body_text,
        body_html,
        attachments,
    })
}
