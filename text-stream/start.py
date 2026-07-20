#!/usr/bin/env python3
"""
text-stream - 一键启动脚本

使用方法：
    python start.py

这将同时启动：
1. text-stream（WebSocket服务，默认端口9876）
2. 前端UI的HTTP服务器（默认端口8080）

启动后访问：http://localhost:8080
"""

import asyncio
import http.server
import os
import sys
import threading
import webbrowser
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from text_stream_system.system import TextStreamSystem


def start_http_server(port=8080, directory=None):
    """启动HTTP服务器来提供前端UI文件"""
    if directory is None:
        directory = project_root / "text_stream_web"
    
    os.chdir(directory)
    
    handler = http.server.SimpleHTTPRequestHandler
    httpd = http.server.HTTPServer(("", port), handler)
    
    print(f"🌐 前端UI已启动: http://localhost:{port}")
    print(f"   服务目录: {directory}")
    
    httpd.serve_forever()


async def main():
    """主函数：启动文本数据流系统和前端UI"""
    
    print("=" * 60)
    print("文本数据流管理系统 - 启动中...")
    print("=" * 60)
    
    # 配置
    ws_port = 9876  # WebSocket服务端口
    http_port = 8080  # HTTP服务端口
    
    # 检查端口是否可用
    import socket
    
    def is_port_available(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('', port))
                return True
            except OSError:
                return False
    
    if not is_port_available(ws_port):
        print(f"❌ 错误: 端口 {ws_port} 已被占用（WebSocket服务）")
        print("   请关闭占用该端口的程序，或修改 start.py 中的 ws_port")
        sys.exit(1)
    
    if not is_port_available(http_port):
        print(f"❌ 错误: 端口 {http_port} 已被占用（HTTP服务）")
        print("   请关闭占用该端口的程序，或修改 start.py 中的 http_port")
        sys.exit(1)
    
    # 启动HTTP服务器（在后台线程中）
    http_thread = threading.Thread(
        target=start_http_server,
        args=(http_port,),
        daemon=True
    )
    http_thread.start()
    
    # 启动文本数据流系统
    print(f"\n🚀 正在启动文本数据流系统（端口: {ws_port}）...")
    
    try:
        system = await TextStreamSystem.start(port=ws_port)
        
        print(f"\n✅ 系统启动成功！")
        print(f"   系统ID: {system.system_id}")
        print(f"   WebSocket端口: {ws_port}")
        print(f"   HTTP端口: {http_port}")
        
        print("\n" + "=" * 60)
        print("🎉 启动完成！")
        print("=" * 60)
        print(f"\n📱 前端UI: http://localhost:{http_port}")
        print(f"🔌 WebSocket: ws://localhost:{ws_port}")
        print("\n按 Ctrl+C 停止系统\n")
        
        # 尝试自动打开浏览器
        try:
            webbrowser.open(f"http://localhost:{http_port}")
            print("🌐 已自动打开浏览器")
        except Exception:
            print("💡 请手动打开浏览器访问: http://localhost:{http_port}")
        
        # 保持运行
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print("\n\n⏹️  正在停止系统...")
            await system.shutdown()
            print("✅ 系统已停止")
            
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 设置日志级别
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 运行主函数
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 再见！")
