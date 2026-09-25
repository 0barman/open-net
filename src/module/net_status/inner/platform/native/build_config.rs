//! Keep native compilation independent of the locally installed SDK version.

use std::io;

// 解析并校验架构对应的 macOS 最低部署版本，避免本机 SDK 版本改变产物的系统要求。
// requested 缺省时 x86_64 使用 10.14、aarch64 使用 11.0；合法显式值保持原字符串。
// 不支持的架构、非 1 至 3 段无符号十进制数字或低于架构下限的版本均返回输入错误。
pub fn deployment_target(arch: &str, requested: Option<&str>) -> io::Result<String> {
    let (minimum, default) = match arch {
        "x86_64" => ((10, 14, 0), "10.14"),
        "aarch64" => ((11, 0, 0), "11.0"),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported macOS architecture: {arch}"),
            ));
        }
    };
    let Some(requested) = requested else {
        return Ok(default.to_owned());
    };
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid MACOSX_DEPLOYMENT_TARGET: {requested:?}"),
        )
    };
    let mut version = (0_u32, 0_u32, 0_u32);
    for (index, part) in requested.split('.').enumerate() {
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        let value = part.parse::<u32>().map_err(|_| invalid())?;
        match index {
            0 => version.0 = value,
            1 => version.1 = value,
            2 => version.2 = value,
            _ => return Err(invalid()),
        }
    }
    if version < minimum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("MACOSX_DEPLOYMENT_TARGET {requested} is below {arch} minimum {default}"),
        ));
    }
    Ok(requested.to_owned())
}
