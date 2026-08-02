# text-stream - 前端UI

纯静态网页应用，用于管理和操作文本数据流系统。

## 功能特性

- WebSocket连接管理
- 流管理（创建、删除、查询、搜索）
- 内容编辑（读取、写入、追加、插入、删除、替换）
- 语义层搜索（搜索、框选搜索）
- 实时订阅（订阅、取消订阅、变更通知）
- 元数据管理（描述、标签、重命名）
- 版本控制（撤销操作）

## 快速开始（推荐）

### 一键启动

```bash
python start.py
```

这将同时启动：
- text-stream（WebSocket服务，端口9876）
- 前端UI的HTTP服务器（端口8080）

启动后会自动打开浏览器访问 http://localhost:8080

### 手动启动

1. 启动文本数据流系统：
   ```python
   from text_stream_system.system import TextStreamSystem
   import asyncio

   system = asyncio.run(TextStreamSystem.start(port=9876))
   ```

2. 在浏览器中打开 `text_stream_web/index.html`

3. 系统会自动连接到 `ws://localhost:9876`

## 配置

通过URL参数配置：

```
index.html?port=9876&token=your_token
```

配置项：
- `port`: WebSocket服务端口（默认：9876）
- `token`: 认证token（可选）

配置会自动保存到本地存储。

## 文件结构

```
text_stream_web/
├── index.html              # 主页面
├── css/
│   └── style.css           # 主样式（含响应式）
├── js/
│   ├── app.js              # 应用入口
│   ├── websocket.js        # WebSocket客户端
│   ├── streams.js          # 流管理逻辑
│   └── subscriptions.js    # 订阅管理
└── assets/
    └── icons/              # 图标资源
```

## 使用说明

### 连接管理

- 点击"连接"按钮手动连接
- 点击"断开"按钮断开连接
- 连接断开后会自动重连（最多5次）

### 流管理

- 左侧面板显示所有流
- 点击流查看详情
- 使用搜索框过滤流
- 点击"+ 新建流"创建新流

### 内容编辑

- "内容"标签页显示流内容
- 使用工具栏按钮执行操作
- 使用搜索框搜索内容
- 点击搜索结果定位到匹配位置

### 订阅管理

- "订阅"标签页管理订阅
- 点击"订阅"按钮订阅流
- 实时显示变更通知
- 点击"取消订阅"取消订阅

### 元数据管理

- "元数据"标签页管理元数据
- 编辑描述并点击"更新"
- 添加/删除标签
- 重命名流

## 技术栈

- HTML5
- CSS3 (CSS Grid/Flexbox)
- ES6+ JavaScript
- WebSocket API

## 浏览器支持

- Chrome 60+
- Firefox 55+
- Safari 11+
- Edge 79+

## 开发

无需构建步骤，直接编辑文件即可。

### 调试

打开浏览器开发者工具（F12），查看控制台日志。

### 扩展

如需添加新功能：

1. 在对应的JS文件中添加逻辑
2. 在HTML中添加UI元素
3. 在CSS中添加样式
4. 在app.js中绑定事件

## 常见问题

### 连接失败

- 检查文本数据流系统是否正在运行
- 检查端口是否正确
- 检查防火墙设置

### 认证失败

- 检查token是否正确
- 检查系统是否启用了认证

### 操作失败

- 检查流是否存在
- 检查是否有写锁冲突
- 查看控制台错误信息

## 许可证

Apache-2.0，见仓库根目录 [LICENSE](../../LICENSE)。
