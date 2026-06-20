# Cube Project 认证集成方案

## 概述

通过 OAuth2 授权码流程，复用 my_demo 系统的用户认证能力，无需修改 my_demo 代码。

## 架构

```
用户浏览器
    │
    ├─ 1. 访问 cube_project (未登录)
    │      └─ 调用 cube_project /api/auth/login
    │             └─ 跳转到 my_demo /api/authorize
    │
    ├─ 2. my_demo 认证
    │      ├─ 已登录 → 生成 code → 重定向 callback
    │      └─ 未登录 → 显示登录页 → 登录后重定向
    │
    ├─ 3. cube_project /callback
    │      ├─ 接收 code
    │      ├─ 调用 my_demo /api/exchange_code 换 token
    │      └─ 返回 token，前端存入 localStorage
    │
    └─ 4. 后续请求
           └─ Authorization: Bearer <token>
```

## 配置

### 关键凭证

| 配置项 | 值 | 来源 |
|--------|-----|------|
| **Client ID** | `system_ard` | my_demo config.py → CLIENTS |
| **Client Secret** | `<从 my_demo CLIENTS["system_ard"] 读取>` | my_demo config.py → CLIENTS["system_ard"] |
| **JWT Secret Key** | `<从 my_demo Settings.SECRET_KEY 读取>` | my_demo config.py → Settings.SECRET_KEY |
| **JWT Algorithm** | `HS256` | my_demo config.py → Settings.ALGORITHM |
| **my_demo 后端地址** | `http://10.3.100.182:5177` | my_demo config.py → Settings.ME_system_url |
| **回调地址** | `http://10.3.100.179:50040/callback` | my_demo config.py → CLIENTS["system_ard"].redirect_uris |

### 环境变量 (`.cube_web.env`)

```bash
# ==================== OAuth2 认证配置 ====================

# OAuth2 客户端配置
CUBE_WEB_AUTH_CLIENT_ID=system_ard
CUBE_WEB_AUTH_CLIENT_SECRET=<my-demo-client-secret>

# my_demo 系统地址
CUBE_WEB_AUTH_MAIN_SYSTEM_URL=http://10.3.100.182:5177

# 回调地址（cube_project 的地址）
CUBE_WEB_AUTH_REDIRECT_URI=http://10.3.100.179:50040/callback

# my_demo 接口路径
CUBE_WEB_AUTH_TOKEN_PATH=/api/exchange_code
CUBE_WEB_AUTH_AUTHORIZE_PATH=/api/authorize
CUBE_WEB_AUTH_LOGOUT_PATH=/api/logout

# JWT 验证配置（必须与 my_demo 完全一致）
CUBE_WEB_AUTH_JWT_SECRET_KEY=<my-demo-jwt-secret>
CUBE_WEB_AUTH_JWT_ALGORITHM=HS256

# 是否强制认证（true=未登录禁止访问，false=允许匿名访问）
CUBE_WEB_AUTH_REQUIRED=true
```

### my_demo 已有配置（无需修改）

```python
# 来源：/home/guanwang/my_demo/backend/app/core/config.py

class Settings(BaseSettings):
    SECRET_KEY: str = "<jwt-secret-from-runtime>"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    ME_system_url: str = "http://10.3.100.182:5177"
    
    CLIENTS: dict = {
        "system_ard": {
            "client_secret": "<client-secret-from-runtime>",
            "redirect_uris": ["http://10.3.100.179:50040/callback"]
        },
        # ... 其他客户端
    }
```

## 代码改动

### 1. 后端 (`cube_web/services/auth_service.py`)

新增函数：
- `get_authorize_url()` - 生成授权跳转 URL
- `exchange_code_for_token()` - 用 code 换 token（已有，确认可用）

### 2. 后端路由 (`cube_web/routes/auth.py`)

新增/修改路由：
- `GET /api/auth/login` - 跳转到 my_demo 授权页
- `GET /api/callback` - OAuth2 回调，处理 code 换 token
- `GET /api/auth/me` - 获取当前用户信息（已有，确认可用）

### 3. 前端状态与跳转 (`cube_web/frontend/src/stores/subUser.js`)

- `redirectToAuth()` 通过后端 `/api/auth/login` 发起 OAuth 跳转
- `exchangeCode()` 调用 `/api/callback` 交换 token
- `logout()` 调用 `/api/logout` 通知上游认证服务

### 4. 前端启动鉴权 (`cube_web/frontend/src/App.vue`)

- 应用启动时加载 `/api/config` 运行时认证配置
- 未登录且 `CUBE_WEB_AUTH_REQUIRED=true` 时自动跳转登录
- `/callback` 返回后从 OAuth `state` 恢复原始页面路径

## 测试验证

```bash
# 1. 访问受保护的 API（未登录应返回 401）
curl -X POST http://localhost:50039/v1/config/get \
  -H "Content-Type: application/json" \
  -d '{}'

# 2. 完整登录流程
#    浏览器访问 http://10.3.100.179:50040
#    → 自动跳转到 my_demo 登录
#    → 登录后自动跳回，token 存入 localStorage

# 3. 使用 token 访问 API
curl -X POST http://localhost:50039/v1/config/get \
  -H "Content-Type: application/json" \
  -d '{}' \
  -H "Authorization: Bearer <your-token>"
```

## 注意事项

| 项目 | 说明 |
|------|------|
| JWT Secret | 必须与 my_demo 的 `SECRET_KEY` 一致 |
| HTTPS | 生产环境必须启用 |
| Token 过期 | 默认 30 分钟，可在 my_demo 配置调整 |
| 跨域 | my_demo 已配置 CORS 白名单 |
| 认证模式 | 当前 `cube_web` 仅消费 `CUBE_WEB_AUTH_REQUIRED` 等 OAuth 运行时配置，不读取 `CUBE_WEB_AUTH_MODE` |

## 文件清单

```
cube_project/
├── docs/
│   └── auth-integration-guide.md    # 本文档
├── cube_web/
│   ├── cube_web/
│   │   ├── services/
│   │   │   └── auth_service.py      # [修改] 添加 OAuth2 逻辑
│   │   ├── routes/
│   │   │   └── auth.py              # [修改] 添加回调路由
│   │   └── .cube_web.env            # [修改] 添加 OAuth2 配置
│   └── frontend/
│       └── src/
│           ├── stores/
│           │   └── subUser.js       # [修改] 前端 OAuth 入口与 token 管理
│           └── App.vue              # [修改] 启动鉴权与 callback 处理
```
