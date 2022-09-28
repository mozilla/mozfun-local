/// inspired by https://github.com/litmus-web/Python-Regex/blob/main/src/lib.rs
use regex::Regex;

use mimalloc::MiMalloc;
use pyo3::prelude::*;

/// Faster memory allocator in Pyo3 context
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Help to avoid recompilation
#[pyclass(name = "VersionTruncator")]
pub struct Matcher {
    major_version_regex: Regex,
    minor_version_regex: Regex,
}

#[pymethods]
impl Matcher {
    #[new]
    fn new() -> Self {
        Matcher {
            major_version_regex: Regex::new(r#"^([0-9]+).*"#).unwrap(),
            minor_version_regex: Regex::new(r#"^([0-9]+[.]?[0-9]+).*"#).unwrap(),
        }
    }

    /// Finds the major version or returns 0
    ///
    /// Args:
    ///     text representation of full version number
    pub fn find_major_version(&self, input: &str) -> String {
        let capture = match self.major_version_regex.captures(input) {
            Some(m) => m.get(1).unwrap().as_str(),
            _ => return "0".to_string(),
        };

        capture.to_string()
    }

    /// Finds the major+minor version or returns 0.0
    ///
    /// Args:
    ///     text representation of full version number
    pub fn find_minor_version(&self, input: &str) -> String {
        let capture;
        if let Some(m) = self.minor_version_regex.captures(input) {
            capture = m.get(1).unwrap().as_str();
        } else {
            return "0.0".to_string();
        };

        capture.to_string()
    }
}

#[pyclass(name = "VersionExtractor")]
pub struct Extractor {
    major_version_regex: Regex,
    minor_version_regex: Regex,
    patch_version_regex: Regex,
}

#[pymethods]
impl Extractor {
    #[new]
    fn new() -> Self {
        Extractor {
            major_version_regex: Regex::new(r#"^([0-9]+).*"#).unwrap(),
            minor_version_regex: Regex::new(r#"^[0-9]+[.]([0-9]+).*"#).unwrap(),
            patch_version_regex: Regex::new(r#"^[0-9]+[.][0-9]+[.]([0-9]+).*"#).unwrap(),
        }
    }

    pub fn extract_version(&self, raw_version: &str, version_to_extract: &str) -> Option<usize> {
        let matcher = match version_to_extract {
            i if i == "major" => &self.major_version_regex,
            i if i == "minor" => &self.minor_version_regex,
            i if i == "patch" => &self.patch_version_regex,
            _ => return None,
        };

        let capture = match matcher.captures(raw_version) {
            Some(c) => c.get(1).unwrap().as_str(),
            _ => return None,
        };

        Some(capture.parse::<usize>().unwrap_or(0))
    }
}

#[pyfunction]
pub fn norm_normalize_os(unnormalized_os: &str) -> PyResult<&str> {
    if unnormalized_os.starts_with("Windows") || unnormalized_os.starts_with("WINNT") {
        return Ok("Windows");
    } else if unnormalized_os.starts_with("Darwin") {
        return Ok("Mac");
    } else if unnormalized_os.starts_with("iOS") || unnormalized_os.contains("iPhone") {
        return Ok("iOS");
    } else if unnormalized_os.starts_with("Android") {
        return Ok("Android");
    } else if unnormalized_os.contains("Linux")
        || unnormalized_os.contains("BSD")
        || unnormalized_os.contains("SunOS")
        || unnormalized_os.contains("Solaris")
    {
        return Ok("Linux");
    }

    return Ok("Other");
}

#[allow(dead_code)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_version() {
        let release = "106.0.1";
        let junk_version = "PerrysNightlyBuild";
        let matcher = Matcher::new();
        assert_eq!(matcher.find_major_version(release), "106".to_string());
        assert_eq!(matcher.find_major_version(junk_version), "0".to_string());
        assert_eq!(matcher.find_minor_version(release), "106.0".to_string());
        assert_eq!(matcher.find_minor_version(junk_version), "0.0".to_string());
    }

    #[test]
    fn test_extract_version() {
        let extractor = Extractor::new();
        assert_eq!(Some(1), extractor.extract_version("16.1.1", "minor"));
        assert_eq!(Some(3), extractor.extract_version("16.03.1", "minor"));
        assert_eq!(Some(16), extractor.extract_version("16.1.1", "major"));
        assert_eq!(None, extractor.extract_version("10", "minor"));
        assert_eq!(
            Some(1),
            extractor.extract_version("5.1.5-ubuntu-foobar", "minor")
        );
        assert_eq!(Some(100), extractor.extract_version("100.01.1", "major"));
        assert_eq!(Some(4), extractor.extract_version("100.04.1", "minor"));
        assert_eq!(
            Some(5),
            extractor.extract_version("5.1.5-ubuntu-foobar", "patch")
        );
        assert_eq!(None, extractor.extract_version("foo-bar", "minor"));
        assert_eq!(Some(4), extractor.extract_version("99.4.1", "minor"));
        assert_eq!(Some(1), extractor.extract_version("99.4.1", "patch"));
        assert_eq!(Some(44), extractor.extract_version("44.1.2", "major"));
    }

    #[test]
    fn test_os_norm() {
        // Desktop OS.
        assert_eq!("Windows", norm_normalize_os("Windows").unwrap());
        assert_eq!("Windows", norm_normalize_os("WINNT").unwrap());
        assert_eq!("Windows", norm_normalize_os("Windows_NT").unwrap());
        assert_eq!("Windows", norm_normalize_os("WindowsNT").unwrap());
        assert_eq!("Mac", norm_normalize_os("Darwin").unwrap());
        assert_eq!("Linux", norm_normalize_os("Linux").unwrap());
        assert_eq!("Linux", norm_normalize_os("GNU/Linux").unwrap());
        assert_eq!("Linux", norm_normalize_os("SunOS").unwrap());
        assert_eq!("Linux", norm_normalize_os("Solaris").unwrap());
        assert_eq!("Linux", norm_normalize_os("FreeBSD").unwrap());
        assert_eq!("Linux", norm_normalize_os("GNU/kFreeBSD").unwrap());
        assert_eq!("Other", norm_normalize_os("AIX").unwrap());
        // Mobile OS.
        assert_eq!("iOS", norm_normalize_os("iOS").unwrap());
        assert_eq!("iOS", norm_normalize_os("iOS?").unwrap());
        assert_eq!("iOS", norm_normalize_os("iPhone").unwrap());
        assert_eq!("iOS", norm_normalize_os("All the iPhones").unwrap());
        assert_eq!("Other", norm_normalize_os("All the iOSes").unwrap());
        assert_eq!("Other", norm_normalize_os("IOS").unwrap());
        assert_eq!("Android", norm_normalize_os("Android").unwrap());
        assert_eq!("Android", norm_normalize_os("Android?").unwrap());
        assert_eq!("Other", norm_normalize_os("All the Androids").unwrap());
        // Other.
        assert_eq!("Other", norm_normalize_os("asdf").unwrap());
    }
}
