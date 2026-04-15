import os
import re
import time
import mmap
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
import tgcrypto
import subprocess
import concurrent.futures
from math import ceil
from utils import progress_bar
from pyrogram import Client, filters
from pyrogram.types import Message
from io import BytesIO
from pathlib import Path  
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode

# Global counter for failed attempts
failed_counter = 0

def duration(filename):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    try:
        return float(result.stdout)
    except:
        return 0

def split_video(file_path, max_size=1.9 * 1024 * 1024 * 1024):
    """Splits video into parts if larger than 1.9GB for Telegram upload"""
    if not os.path.exists(file_path) or os.path.getsize(file_path) <= max_size:
        return [file_path]

    print(f"📂 Large file detected ({os.path.getsize(file_path) / (1024**3):.2f} GB). Splitting...")
    base_name, ext = os.path.splitext(file_path)
    
    # Get total duration
    duration_cmd = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{file_path}"'
    res = subprocess.run(duration_cmd, shell=True, capture_output=True, text=True)
    try:
        total_duration = float(res.stdout)
    except:
        return [file_path]
    
    num_parts = int(os.path.getsize(file_path) // max_size) + 1
    part_duration = total_duration / num_parts
    parts = []

    for i in range(num_parts):
        start_time = i * part_duration
        output_part = f"{base_name}_Part{i+1}{ext}"
        # Using '-c copy' for instant splitting without quality loss
        split_cmd = f'ffmpeg -i "{file_path}" -ss {start_time} -t {part_duration} -c copy "{output_part}" -y'
        os.system(split_cmd)
        if os.path.exists(output_part):
            parts.append(output_part)

    os.remove(file_path) # Delete original bulky file
    return parts

def get_mps_and_keys(api_url):
    response = requests.get(api_url)
    response_json = response.json()
    mpd = response_json.get('MPD')
    keys = response_json.get('KEYS')
    return mpd, keys
   
def exec(cmd):
    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output = process.stdout.decode()
    print(output)
    return output

def pull_run(work, cmds):
    with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
        print("Waiting for tasks to complete")
        fut = executor.map(exec, cmds)
        
async def aio(url, name):
    k = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(k, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return k

async def download(url, name):
    ka = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(ka, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return ka

def parse_vid_info(info):
    info = info.strip().split("\n")
    new_info = []
    temp = []
    for i in info:
        if "[" not in i and '---' not in i:
            i = " ".join(i.split())
            parts = i.split("|")[0].split(" ", 2)
            try:
                if "RESOLUTION" not in parts[2] and parts[2] not in temp and "audio" not in parts[2]:
                    temp.append(parts[2])
                    new_info.append((parts[0], parts[2]))
            except:
                pass
    return new_info

def vid_info(info):
    info = info.strip().split("\n")
    new_info = dict()
    temp = []
    for i in info:
        if "[" not in i and '---' not in i:
            i = " ".join(i.split())
            parts = i.split("|")[0].split(" ", 3)
            try:
                if "RESOLUTION" not in parts[2] and parts[2] not in temp and "audio" not in parts[2]:
                    temp.append(parts[2])
                    new_info.update({f'{parts[2]}': f'{parts[0]}'})
            except:
                pass
    return new_info

async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        cmd1 = f'yt-dlp -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --concurrent-fragments 10 --external-downloader aria2c --downloader-args "aria2c:-x 16 -j 16 -s 10 -k 1M" "{mpd_url}"'
        os.system(cmd1)
        
        avDir = list(output_path.iterdir())
        video_decrypted = audio_decrypted = False

        for data in avDir:
            if data.suffix == ".mp4" and not video_decrypted:
                cmd2 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                os.system(cmd2)
                if (output_path / "video.mp4").exists(): video_decrypted = True
                data.unlink()
            elif data.suffix == ".m4a" and not audio_decrypted:
                cmd3 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                os.system(cmd3)
                if (output_path / "audio.m4a").exists(): audio_decrypted = True
                data.unlink()

        if not video_decrypted or not audio_decrypted:
            raise FileNotFoundError("Decryption failed.")

        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
        os.system(cmd4)
        
        for f in ["video.mp4", "audio.m4a"]:
            if (output_path / f).exists(): (output_path / f).unlink()
        
        return str(output_path / f"{output_name}.mp4")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

async def run(cmd):
    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode == 1: return False
    return f'[stdout]\n{stdout.decode()}' if stdout else f'[stderr]\n{stderr.decode()}'

def old_download(url, file_name, chunk_size = 1024 * 1024):
    if os.path.exists(file_name): os.remove(file_name)
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk: fd.write(chunk)
    return file_name

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0: break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"

def time_name():
    return f"{datetime.datetime.now().strftime('%Y-%m-%d %H%M%S')}.mp4"

async def download_video(url, cmd, name):
    global failed_counter
    download_cmd = f'{cmd} -R 25 --fragment-retries 25 --concurrent-fragments 10 --external-downloader aria2c --downloader-args "aria2c:-x 10 -j 16 -s 10 -k 1M"'
    
    print(f"Executing: {download_cmd}")
    k = subprocess.run(download_cmd, shell=True)
    
    if "visionias" in cmd and k.returncode != 0 and failed_counter <= 10:
        failed_counter += 1
        await asyncio.sleep(5)
        return await download_video(url, cmd, name)
    
    failed_counter = 0
    base_name = name.split(".")[0]
    for ext in [".mp4", ".mkv", ".webm", ".mp4.webm"]:
        if os.path.isfile(f"{base_name}{ext}"): return f"{base_name}{ext}"
        if os.path.isfile(f"{name}{ext}"): return f"{name}{ext}"
    return name if os.path.isfile(name) else f"{base_name}.mp4"

async def send_doc(bot: Client, m: Message, cc, ka, cc1, prog, count, name, channel_id):
    reply = await bot.send_message(channel_id, f"Downloading pdf:\n<code>{name}</code>")
    await bot.send_document(channel_id, ka, caption=cc1)
    await reply.delete()
    if os.path.exists(ka): os.remove(ka)

def decrypt_file(file_path, key):  
    if not os.path.exists(file_path): return False  
    with open(file_path, "r+b") as f:  
        num_bytes = min(28, os.path.getsize(file_path))  
        with mmap.mmap(f.fileno(), length=num_bytes, access=mmap.ACCESS_WRITE) as mmapped_file:  
            for i in range(num_bytes):  
                mmapped_file[i] ^= ord(key[i]) if i < len(key) else i 
    return True  

async def send_vid(bot: Client, m: Message, cc, filename, vidwatermark, thumb, name, prog, channel_id):
    if prog: await prog.delete()
    
    # Check for Splitting (2GB+ Limit)
    video_parts = split_video(filename)
    
    for i, part in enumerate(video_parts):
        # Generate thumbnail for each part
        part_thumb = f"{part}.jpg"
        subprocess.run(f'ffmpeg -i "{part}" -ss 00:00:10 -vframes 1 "{part_thumb}" -y', shell=True)
        
        # Determine thumbnail path
        final_thumbnail = part_thumb if thumb == "/d" else thumb
        
        # Handle Watermark for each part
        w_part = part
        if vidwatermark != "/d":
            w_part = f"w_{part}"
            font_path = "vidwater.ttf"
            subprocess.run(f'ffmpeg -i "{part}" -vf "drawtext=fontfile={font_path}:text=\'{vidwatermark}\':fontcolor=white@0.3:fontsize=h/6:x=(w-text_w)/2:y=(h-text_h)/2" -preset superfast -codec:a copy "{w_part}" -y', shell=True)

        status_text = f"**📩 Uploading Video 📩:-**\n<blockquote>**{name}**</blockquote>"
        if len(video_parts) > 1:
            status_text += f"\n📦 **Part:** `{i+1}/{len(video_parts)}`"
            
        reply1 = await bot.send_message(channel_id, status_text)
        
        dur = int(duration(w_part))
        start_time = time.time()

        try:
            caption_text = f"{cc}\n\n📦 **Part:** `{i+1}`" if len(video_parts) > 1 else cc
            await bot.send_video(
                channel_id, 
                w_part, 
                caption=caption_text, 
                supports_streaming=True, 
                height=720, 
                width=1280, 
                thumb=final_thumbnail if os.path.exists(str(final_thumbnail)) else None, 
                duration=dur, 
                progress=progress_bar, 
                progress_args=(reply1, start_time)
            )
        except Exception as e:
            print(f"Upload error: {e}")
            await bot.send_document(channel_id, w_part, caption=cc)
        
        # Clean up files for this part
        for f in [w_part, part, part_thumb]:
            if os.path.exists(f): 
                try: os.remove(f)
                except: pass
        
        await reply1.delete()
