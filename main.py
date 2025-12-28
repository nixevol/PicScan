from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import zipfile
import os
import sys
import time
import asyncio
from pathlib import Path
import json
from typing import Dict, Optional, List
from datetime import datetime
import shutil
import threading

from data_processor import DataProcessor

app = FastAPI()

# 任务状态存储
tasks: Dict[str, Dict] = {}

# 线程锁，用于安全更新任务状态
task_locks: Dict[str, threading.Lock] = {}


def get_base_path():
    """获取基础路径，支持打包后的exe环境"""
    if getattr(sys, 'frozen', False):
        # 打包后的exe环境
        return Path(sys.executable).parent
    else:
        # 开发环境
        return Path(__file__).parent


def get_resource_path(relative_path):
    """获取资源文件路径，支持打包后的exe环境"""
    if getattr(sys, 'frozen', False):
        # 打包后，资源文件在 _MEIPASS 目录中
        base_path = Path(sys._MEIPASS)
    else:
        base_path = Path(__file__).parent
    return base_path / relative_path


# 基础路径和数据目录
BASE_PATH = get_base_path()
DATA_DIR = BASE_PATH / 'data'
DATA_DIR.mkdir(exist_ok=True)


@app.get("/", response_class=HTMLResponse)
async def read_root():
    html_path = get_resource_path('index.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        return f.read()


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), xlsx_filename: Optional[str] = None):
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="只支持ZIP文件")
    
    # 生成13位时间戳作为任务ID
    task_id = str(int(time.time() * 1000))
    task_dir = DATA_DIR / task_id
    task_dir.mkdir(exist_ok=True)
    
    # 保存ZIP文件
    zip_path = task_dir / file.filename
    with open(zip_path, 'wb') as f:
        shutil.copyfileobj(file.file, f)
    
    # 解压ZIP
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(task_dir)
        os.remove(zip_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"解压失败: {str(e)}")
    
    # 初始化任务状态
    tasks[task_id] = {
        'status': 'processing',
        'progress': 0,
        'message': '开始处理...',
        'result': None,
        'error': None,
        'partial_results': [],  # 存储已解析的部分结果
        'total_rows': 0,        # 总行数
        'processed_rows': 0,    # 已处理行数
        'xlsx_filename': xlsx_filename,  # 用户指定的xlsx文件名
        'cancelled': False      # 取消标志
    }
    task_locks[task_id] = threading.Lock()
    
    # 异步处理
    asyncio.create_task(process_task(task_id, task_dir))
    
    return {'task_id': task_id}


@app.get("/api/task/{task_id}")
async def get_task_status(task_id: str, last_index: int = 0):
    """获取任务状态，支持增量获取结果
    
    Args:
        task_id: 任务ID
        last_index: 上次已获取的结果数量，用于增量获取
    """
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks[task_id]
    
    # 获取增量结果（从last_index开始的新数据）
    with task_locks.get(task_id, threading.Lock()):
        new_results = task['partial_results'][last_index:] if task['partial_results'] else []
    
    return {
        'status': task['status'],
        'progress': task['progress'],
        'message': task['message'],
        'result': task['result'],
        'error': task['error'],
        'new_results': new_results,  # 增量结果
        'total_results': len(task['partial_results']),  # 当前已解析的总数
        'total_rows': task['total_rows'],  # 总行数
        'processed_rows': task['processed_rows']  # 已处理行数
    }


@app.post("/api/cancel/{task_id}")
async def cancel_task(task_id: str):
    """取消正在处理的任务"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks[task_id]
    
    if task['status'] != 'processing':
        raise HTTPException(status_code=400, detail="任务已完成或已取消")
    
    with task_locks.get(task_id, threading.Lock()):
        tasks[task_id]['cancelled'] = True
        tasks[task_id]['message'] = '正在取消...'
    
    return {'message': '取消请求已发送'}


@app.get("/api/download/{task_id}")
async def download_result(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    result_file = DATA_DIR / task_id / 'result.csv'
    if not result_file.exists():
        raise HTTPException(status_code=404, detail="结果文件不存在")
    
    return FileResponse(
        result_file,
        filename='result.csv',
        media_type='text/csv'
    )


@app.get("/api/download_partial/{task_id}")
async def download_partial_result(task_id: str):
    """下载已解析的部分数据（处理过程中可用）"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    task = tasks[task_id]
    
    with task_locks.get(task_id, threading.Lock()):
        partial_results = task['partial_results'].copy()
    
    if not partial_results:
        raise HTTPException(status_code=404, detail="暂无已解析的数据")
    
    # 生成CSV内容
    import csv
    import io
    
    fieldnames = ['工单号', '经度', '纬度', '上传速率Mbps', '下载速率Mbps', 
                 'ECI', 'RSRP', 'SINR', 'NR-CI', 'SS-RSRP', 'SS-SINR']
    
    output = io.StringIO()
    # 添加 BOM 以支持 Excel 正确识别中文
    output.write('\ufeff')
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for result in partial_results:
        writer.writerow({k: result.get(k, '') for k in fieldnames})
    
    csv_content = output.getvalue()
    output.close()
    
    from fastapi.responses import Response
    return Response(
        content=csv_content,
        media_type='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f'attachment; filename=partial_result_{len(partial_results)}.csv'
        }
    )


async def process_task(task_id: str, task_dir: Path):
    try:
        tasks[task_id]['message'] = '正在查找Excel文件...'
        tasks[task_id]['progress'] = 5
        
        # 获取用户指定的xlsx文件名
        xlsx_filename = tasks[task_id].get('xlsx_filename')
        
        if xlsx_filename:
            # 用户指定了文件名，查找该文件
            excel_files = list(task_dir.rglob(xlsx_filename))
            if not excel_files:
                # 尝试模糊匹配（文件名包含用户输入的字符串）
                all_xlsx = list(task_dir.rglob('*.xlsx'))
                excel_files = [f for f in all_xlsx if xlsx_filename in f.name]
            if not excel_files:
                raise Exception(f"未找到指定的Excel文件: {xlsx_filename}")
            excel_file = excel_files[0]
        else:
            # 自动查找第一个xlsx文件
            excel_files = list(task_dir.rglob('*.xlsx'))
            if not excel_files:
                raise Exception("未找到Excel文件")
            excel_file = excel_files[0]
        
        tasks[task_id]['message'] = f'找到Excel文件: {excel_file.name}'
        tasks[task_id]['progress'] = 10
        
        # 处理数据
        processor = DataProcessor(task_dir)
        tasks[task_id]['message'] = '正在提取图片和解析数据...'
        tasks[task_id]['progress'] = 15
        
        # 定义进度回调函数
        def progress_callback(current: int, total: int, result: dict) -> bool:
            """每处理完一行就调用此回调
            
            Returns:
                bool: True 继续处理，False 停止处理（被取消）
            """
            with task_locks[task_id]:
                # 检查是否被取消
                if tasks[task_id]['cancelled']:
                    return False
                
                tasks[task_id]['partial_results'].append(result)
                tasks[task_id]['total_rows'] = total
                tasks[task_id]['processed_rows'] = current
                # 进度从15%到90%之间按行数计算
                progress = 15 + int((current / total) * 75)
                tasks[task_id]['progress'] = progress
                tasks[task_id]['message'] = f'正在解析: {current}/{total} - 工单号: {result.get("工单号", "")}'
                return True
        
        # 使用回调处理Excel
        results = await asyncio.to_thread(processor.process_excel, excel_file, progress_callback)
        
        # 检查是否被取消
        if tasks[task_id]['cancelled']:
            tasks[task_id]['status'] = 'cancelled'
            tasks[task_id]['message'] = f'已取消，已解析 {len(tasks[task_id]["partial_results"])} 条数据'
            tasks[task_id]['result'] = len(tasks[task_id]['partial_results'])
            # 保存已解析的部分结果
            if tasks[task_id]['partial_results']:
                result_file = task_dir / 'result.csv'
                processor.save_results(tasks[task_id]['partial_results'], result_file)
            return
        
        tasks[task_id]['message'] = '正在保存结果...'
        tasks[task_id]['progress'] = 95
        
        # 保存结果CSV
        result_file = task_dir / 'result.csv'
        processor.save_results(results, result_file)
        
        tasks[task_id]['status'] = 'completed'
        tasks[task_id]['progress'] = 100
        tasks[task_id]['message'] = '处理完成'
        tasks[task_id]['result'] = len(results)
        
    except Exception as e:
        tasks[task_id]['status'] = 'error'
        tasks[task_id]['error'] = str(e)
        tasks[task_id]['message'] = f'处理失败: {str(e)}'


if __name__ == "__main__":
    import uvicorn
    import webbrowser
    import threading
    
    def open_browser():
        """延迟打开浏览器"""
        import time
        time.sleep(1.5)
        webbrowser.open('http://localhost:8000')
    
    # 在后台线程中打开浏览器
    threading.Thread(target=open_browser, daemon=True).start()
    
    print("=" * 50)
    print("固移工单数据处理工具")
    print("=" * 50)
    print("服务已启动，正在打开浏览器...")
    print("如果浏览器没有自动打开，请手动访问: http://localhost:8000")
    print("按 Ctrl+C 关闭服务")
    print("=" * 50)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)

