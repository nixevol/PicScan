"""
打包脚本 - 将程序打包为exe
使用方法：python build.py
"""
import subprocess
import sys
import os
from pathlib import Path

def main():
    # 确保在正确的目录
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    print("=" * 50)
    print("固移工单数据处理工具 - 打包脚本")
    print("=" * 50)
    
    # 检查并安装 PyInstaller
    print("\n[1/3] 检查 PyInstaller...")
    try:
        import PyInstaller
        print(f"  PyInstaller 已安装: {PyInstaller.__version__}")
    except ImportError:
        print("  正在安装 PyInstaller...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])
        print("  PyInstaller 安装完成")
    
    # 构建 PyInstaller 命令
    print("\n[2/3] 开始打包...")
    
    cmd = [
        sys.executable,
        '-m', 'PyInstaller',
        '--name=固移工单数据处理工具',
        '--onedir',  # 打包为目录（包含所有依赖），如果需要打包为单个文件，则使用 --onefile，但这样会导致exe文件很大，启动很慢
        '--console',  # 显示控制台（方便查看日志）
        '--noconfirm',  # 覆盖已有输出
        # 添加数据文件
        '--add-data=index.html;.',
        '--add-data=.EasyOCR;.EasyOCR',  # 添加EasyOCR模型文件
        # 隐藏导入
        '--hidden-import=uvicorn.logging',
        '--hidden-import=uvicorn.loops',
        '--hidden-import=uvicorn.loops.auto',
        '--hidden-import=uvicorn.protocols',
        '--hidden-import=uvicorn.protocols.http',
        '--hidden-import=uvicorn.protocols.http.auto',
        '--hidden-import=uvicorn.protocols.websockets',
        '--hidden-import=uvicorn.protocols.websockets.auto',
        '--hidden-import=uvicorn.lifespan',
        '--hidden-import=uvicorn.lifespan.on',
        '--hidden-import=uvicorn.lifespan.off',
        '--hidden-import=easyocr',
        '--hidden-import=torch',
        '--hidden-import=torchvision',
        '--hidden-import=PIL',
        '--hidden-import=cv2',
        '--hidden-import=numpy',
        '--hidden-import=openpyxl',
        '--hidden-import=multipart',
        # 收集所有 easyocr 数据
        '--collect-all=easyocr',
        # 入口文件
        'main.py'
    ]
    
    print("  执行命令:")
    print(f"  {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("\n[3/3] 打包完成!")
        print("=" * 50)
        print(f"输出目录: {script_dir / 'dist' / '固移工单数据处理工具'}")
        print("\n使用说明:")
        print("1. 进入 dist/固移工单数据处理工具 目录")
        print("2. 运行 固移工单数据处理工具.exe")
        print("3. 打开浏览器访问 http://localhost:8000")
        print("=" * 50)
    else:
        print("\n打包失败，请检查错误信息")
        sys.exit(1)


if __name__ == '__main__':
    main()

