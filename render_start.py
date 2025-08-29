#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Render.com 部署启动脚本
适配Render平台的环境和端口要求
"""

import os
import uvicorn
from siliconflow_pool import app

if __name__ == "__main__":
    # Render平台会提供PORT环境变量，默认为10000
    port = int(os.environ.get("PORT", 10000))

    # 启动应用
    uvicorn.run(
        "siliconflow_pool:app",
        host="0.0.0.0",
        port=port,
        workers=1,  # Render免费版建议使用单进程
        access_log=True,
        log_level="info",
    )
