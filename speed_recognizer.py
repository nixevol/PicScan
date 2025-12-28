import os
import sys
import re
from pathlib import Path
import json
import csv
import warnings
import shutil
import hashlib
import sqlite3
from datetime import datetime
from typing import Optional, Dict
warnings.filterwarnings('ignore')

try:
    import easyocr
except ImportError:
    print("错误: 请安装 easyocr")
    exit(1)


def get_model_dir():
    """获取模型目录，支持打包后的exe环境"""
    if getattr(sys, 'frozen', False):
        # 打包后的exe环境，模型放在exe同级目录
        base_path = Path(sys.executable).parent
    else:
        # 开发环境
        base_path = Path(__file__).parent
    return str(base_path / '.EasyOCR')


def get_cache_db_path():
    """获取缓存数据库路径，支持打包后的exe环境"""
    if getattr(sys, 'frozen', False):
        base_path = Path(sys.executable).parent
    else:
        base_path = Path(__file__).parent
    return base_path / 'image_cache.db'


def calculate_image_hash(image_path: str) -> str:
    """计算图片文件的MD5哈希值"""
    hash_md5 = hashlib.md5()
    try:
        with open(image_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return ""


class ImageCache:
    """图片识别结果缓存管理器"""
    
    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = get_cache_db_path()
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS image_cache (
                image_hash TEXT PRIMARY KEY,
                upload_speed REAL,
                download_speed REAL,
                recognized_text TEXT,
                created_at TEXT,
                last_used_at TEXT
            )
        ''')
        # 创建索引以提高查询速度
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_image_hash ON image_cache(image_hash)
        ''')
        conn.commit()
        conn.close()
    
    def get(self, image_hash: str) -> Optional[Dict]:
        """从缓存获取识别结果"""
        if not image_hash:
            return None
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT upload_speed, download_speed, recognized_text
            FROM image_cache
            WHERE image_hash = ?
        ''', (image_hash,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # 更新最后使用时间
            self._update_last_used(image_hash)
            return {
                'upload_speed': row[0],
                'download_speed': row[1],
                'recognized_text': row[2]
            }
        return None
    
    def set(self, image_hash: str, upload_speed: Optional[float], 
            download_speed: Optional[float], recognized_text: str = ""):
        """保存识别结果到缓存"""
        if not image_hash:
            return
        
        now = datetime.now().isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO image_cache 
            (image_hash, upload_speed, download_speed, recognized_text, created_at, last_used_at)
            VALUES (?, ?, ?, ?, 
                COALESCE((SELECT created_at FROM image_cache WHERE image_hash = ?), ?),
                ?)
        ''', (image_hash, upload_speed, download_speed, recognized_text, 
              image_hash, now, now))
        conn.commit()
        conn.close()
    
    def _update_last_used(self, image_hash: str):
        """更新最后使用时间"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE image_cache 
            SET last_used_at = ?
            WHERE image_hash = ?
        ''', (datetime.now().isoformat(), image_hash))
        conn.commit()
        conn.close()
    
    def get_stats(self) -> Dict:
        """获取缓存统计信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM image_cache')
        total = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM image_cache WHERE upload_speed IS NOT NULL OR download_speed IS NOT NULL')
        with_result = cursor.fetchone()[0]
        conn.close()
        return {
            'total_cached': total,
            'with_result': with_result
        }


class SpeedRecognizer:
    def __init__(self, model_dir=None, enable_cache=True):
        if model_dir is None:
            model_dir = get_model_dir()
        os.makedirs(model_dir, exist_ok=True)

        # 在打包后的环境中优先将内置模型释放到可写目录
        if getattr(sys, 'frozen', False):
            bundled_base = Path(getattr(sys, '_MEIPASS', Path(model_dir).parent))
            bundled_models = bundled_base / '.EasyOCR'
            target_dir = Path(model_dir)
            try:
                if bundled_models.exists() and (not any(target_dir.iterdir())):
                    shutil.copytree(bundled_models, target_dir, dirs_exist_ok=True)
            except Exception:
                # 静默失败，后续由 easyocr 自行下载
                pass

        self.reader = easyocr.Reader(['ch_sim', 'en'], gpu=False, verbose=False, model_storage_directory=model_dir)
        self.enable_cache = enable_cache
        self.cache = ImageCache() if enable_cache else None

    def extract_speed(self, text):
        result = {'upload_speed': None, 'download_speed': None}
        
        pattern1 = r'上传速度\s+下载速度\s+(\d+\.?\d*)\s+(\d+\.?\d*)'
        match1 = re.search(pattern1, text)
        if match1:
            result['upload_speed'] = float(match1.group(1))
            result['download_speed'] = float(match1.group(2))
            return result
        
        upload_patterns = [
            r'上传速度[：:]\s*(\d+\.?\d*)\s*[Mm]bps',
            r'上传速度\s+(\d+\.?\d*)\s*[Mm]bps',
        ]
        
        download_patterns = [
            r'下载速度[：:]\s*(\d+\.?\d*)\s*[Mm]bps',
            r'下载速度\s+(\d+\.?\d*)\s*[Mm]bps',
        ]
        
        for pattern in upload_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['upload_speed'] = float(match.group(1))
                break
        
        for pattern in download_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result['download_speed'] = float(match.group(1))
                break
        
        return result

    def recognize_image(self, image_path):
        if not os.path.exists(image_path):
            return {'upload_speed': None, 'download_speed': None}
        
        # 计算图片哈希
        image_hash = calculate_image_hash(image_path)
        
        # 先查缓存
        if self.enable_cache and self.cache and image_hash:
            cached_result = self.cache.get(image_hash)
            if cached_result:
                return {
                    'upload_speed': cached_result['upload_speed'],
                    'download_speed': cached_result['download_speed']
                }
        
        # 缓存未命中，进行识别
        try:
            results = self.reader.readtext(image_path)
            text = ' '.join([item[1] for item in results])
            speed_result = self.extract_speed(text)
            
            # 保存到缓存（仅保存成功识别的结果，即使速度值为None也保存，因为OCR识别成功了）
            if self.enable_cache and self.cache and image_hash:
                self.cache.set(
                    image_hash,
                    speed_result.get('upload_speed'),
                    speed_result.get('download_speed'),
                    recognized_text=text
                )
            
            return speed_result
        except Exception as e:
            # 识别失败不保存到缓存，下次遇到会重新尝试识别
            return {'upload_speed': None, 'download_speed': None}

    def recognize_directory(self, directory='images'):
        path = Path(directory)
        if not path.exists():
            return []
        
        images = set()
        for ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp']:
            images.update(path.glob(f'*{ext}'))
            images.update(path.glob(f'*{ext.upper()}'))
        
        results = []
        for img in sorted(images):
            speeds = self.recognize_image(str(img))
            upload = speeds.get('upload_speed')
            download = speeds.get('download_speed')
            print(f"{img.name}: 上传={upload} Mbps, 下载={download} Mbps")
            results.append({
                'image_path': str(img),
                'upload_speed': upload,
                'download_speed': download
            })
        
        return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', '-d', default='images')
    parser.add_argument('--image', '-i')
    parser.add_argument('--output', '-o')
    args = parser.parse_args()
    
    recognizer = SpeedRecognizer()
    
    if args.image:
        speeds = recognizer.recognize_image(args.image)
        print(f"{Path(args.image).name}: 上传={speeds.get('upload_speed')} Mbps, 下载={speeds.get('download_speed')} Mbps")
        results = [{'image_path': args.image, **speeds}]
    else:
        results = recognizer.recognize_directory(args.dir)
    
    # 输出CSV到result目录
    result_dir = Path('result')
    result_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = result_dir / f'result_{timestamp}.csv'
    
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['文件名', '上传速率', '下载速率'])
        for r in results:
            filename = Path(r['image_path']).name
            upload = r.get('upload_speed', '')
            download = r.get('download_speed', '')
            writer.writerow([filename, upload, download])
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
