import os
import sys
import re
from pathlib import Path
import json
import csv
import warnings
from datetime import datetime
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


class SpeedRecognizer:
    def __init__(self, model_dir=None):
        if model_dir is None:
            model_dir = get_model_dir()
        os.makedirs(model_dir, exist_ok=True)
        self.reader = easyocr.Reader(['ch_sim', 'en'], gpu=False, verbose=False, model_storage_directory=model_dir)

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
        
        try:
            results = self.reader.readtext(image_path)
            text = ' '.join([item[1] for item in results])
            return self.extract_speed(text)
        except:
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
