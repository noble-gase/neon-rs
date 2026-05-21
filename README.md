# 氖-Ne

[<img alt="crates.io" src="https://img.shields.io/crates/v/kr.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/kr)
[<img alt="MIT" src="http://img.shields.io/badge/license-MIT-brightgreen.svg?style=for-the-badge" height="20">](http://opensource.org/licenses/MIT)

[氪-Kr] Rust开发工具包

## 安装

```shell
cargo add kr --features macros
```

## kr-core

| 模块   | 说明                                      |
| ------ | ----------------------------------------- |
| crypto | 封装 Hash 和 AES 相关方法                 |
| helper | 一些辅助方法：Time、Redis                 |
| mutex  | 基于 Redis 的分布式锁                     |
| redix  | 基于 `bb8` 的 Redis 连接池初始化封装      |
| sql    | DB初始化 和 基于 `sea-query` 的 curd 封装 |

#### 说明

- AES
  - CBC
  - ECB
  - GCM

⚠️ `aes` 相关功能依赖 `openssl`

## kr-macros

#### 派生宏：Model

- 使用

```rust
#[derive(Model)]
#[model(UserLite !(email, phone))] // 排除字段
#[model(UserBrief (id, name), derive(Copy, Debug))] // 包含字段
pub struct User {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,

    pub email: String,
    pub phone: String,
    pub created_at: String,
    pub updated_at: String,
}
```

- 生成代码

```rust
#[derive(sqlx::FromRow)]
pub struct UserLite {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,

    pub created_at: String,
    pub updated_at: String,
}

#[derive(sqlx::FromRow, Copy, Debug)]
pub struct UserBrief {
    pub id: i64,

    #[sqlx(rename = "username")]
    pub name: String,
}
```

👉 具体使用可以参考 [rnx](https://crates.io/crates/rnx)

**Enjoy 😊**
