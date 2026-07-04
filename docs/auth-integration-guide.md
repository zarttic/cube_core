# 认证集成方案

## 概述

通过 OAuth2 授权码流程，复用统一认证系统的用户认证能力。`cube_web` 作为 OAuth2 客户端，
认证服务作为授权服务器。

## 架构

```
用户浏览器
    │
    ├─ 1. 访问 cube_project (未登录)
    │      └─ 调用 /api/auth/login
    │             └─ 跳转到认证服务 /api/authorize
    │
    ├─ 2. 认证服务认证
    │      ├─ 已登录 → 生成 code → 重定向 callback
    │      └─ 未登录 → 显示登录页 → 登录后重定向
    │
    ├─ 3. cube_project /callback
    │      ├─ 接收 code
    │      ├─ 调用认证服务 /api/exchange_code 换 token
    │      └─ 返回 token，前端存入 localStorage
    │
    └─ 4. 后续请求
           └─ Authorization: Bearer <token>
```

## 运行时配置

在 `.cube_web.env` 中配置：

```bash
# OAuth2 客户端配置
CUBE_WEB_AUTH_CLIENT_ID=system_ard
CUBE_WEB_AUTH_CLIENT_SECRET=<client-secret>

# 认证服务地址
CUBE_WEB_AUTH_MAIN_SYSTEM_URL=http://<auth-host>:5177

# 回调地址（cube_project 的地址）
CUBE_WEB_AUTH_REDIRECT_URI=http://<cube-web-host>:50040/callback

# 认证服务接口路径
CUBE_WEB_AUTH_TOKEN_PATH=/api/exchange_code
CUBE_WEB_AUTH_AUTHORIZE_PATH=/api/authorize
CUBE_WEB_AUTH_LOGOUT_PATH=/api/logout

# JWT 验证配置（必须与认证服务完全一致）
CUBE_WEB_AUTH_JWT_SECRET_KEY=<jwt-secret>
CUBE_WEB_AUTH_JWT_ALGORITHM=HS256

# 是否强制认证（true=未登录禁止访问，false=允许匿名访问）
CUBE_WEB_AUTH_REQUIRED=true
```

## 测试验证

```bash
# 1. 访问受保护的 API（未登录应返回 401）
curl -X POST http://localhost:50039/v1/config/get \
  -H "Content-Type: application/json" -d '{}'

# 2. 使用 token 访问 API
curl -X POST http://localhost:50039/v1/config/get \
  -H "Content-Type: application/json" -d '{}' \
  -H "Authorization: Bearer <your-token>"
```

## 注意事项

| 项目 | 说明 |
|------|------|
| JWT Secret | 必须与认证服务的 `SECRET_KEY` 一致 |
| HTTPS | 生产环境必须启用 |
| Token 过期 | 默认 30 分钟，可在认证服务配置调整 |
| 跨域 | 认证服务已配置 CORS 白名单 |
