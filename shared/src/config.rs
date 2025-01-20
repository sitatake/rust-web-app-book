use anyhow::Result;

pub struct AppConfig {
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub auth: AuthConfig,
}

pub struct RedisConfig {
    pub host: String,
    pub port: u16,
}

impl AppConfig {
    pub fn new() -> Result<Self> {
        let database = DatabaseConfig {
            host: std::env::var("DATABASE_HOST")?,
            port: std::env::var("DATABASE_PORT")?.parse()?,
            username: std::env::var("DATABASE_USERNAME")?,
            password: std::env::var("DATABASE_PASSWORD")?,
            database: std::env::var("DATABASE_NAME")?,
        };

        //環境変数をよう見込んで各設定値のデータを作成する
        let redis = RedisConfig {
            host: std::env::var("REDIS_HOST")?,
            port: std::env::var("REDIS_PORT")?.parse::<u16>()?,
        };
        let auth = AuthConfig {
            ttl: std::env::var("AUTH_TOKEN_TTL")?.parse::<u64>()?,
        };
        Ok(Self {
            database,
            redis,
            auth,
        })
    }
}

pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

pub struct AuthConfig {
    pub ttl: u64,
}
