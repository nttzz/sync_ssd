#!/usr/bin/env python3
import os
import shutil
from datetime import datetime, timedelta
import subprocess
import logging
from pathlib import Path
import sys


log_file_name = datetime.now().strftime('mylogfile_%s_%H_%M_%d_%m_%Y.log')
logging.basicConfig(
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S',
                    level=logging.DEBUG,
                        handlers=[
        logging.FileHandler(f"/home/autera-admin/python/logs/{log_file_name}", mode='w+'),
        logging.StreamHandler()
    ])


SSD_MOUNT_PATH = '/mnt/dsu0/'
LOCK_FILE = '/home/autera-admin/python/sync.lock'


def get_tag_file_name(raw_folder: str):
    for file_name in os.listdir(raw_folder):
        # print(file_name)
        if file_name.endswith('.txt'):
            return file_name
    return None


def is_completed(raw_name: str):
    str_raw_time = raw_name.split('@')[1]
    raw_time = datetime.strptime(str_raw_time, '%Y%m%d_%H%M%S%f')
    # print(datetime.now() - raw_time)

    if datetime.now() - raw_time > timedelta(minutes=7):
        return True
    else:
        return False

def get_list_completed_raw(car_folder: str):
    list_raw_names = os.listdir(car_folder)
    list_completed_raw = []
    for raw_name in list_raw_names:
        if '@' in raw_name and is_completed(raw_name):
            list_completed_raw.append(raw_name)
    return list_completed_raw


def move_parent_folder_of_txt_to_critical(source_folder, critical_folder_on_ssd_autera, dst_external_ssd_folder_name):
    # Create directory "critical" if it doesn't already exist
    if not os.path.exists(critical_folder_on_ssd_autera):
        os.makedirs(critical_folder_on_ssd_autera)
        print(f'create folder "{critical_folder_on_ssd_autera}" success')
    else:
        print(f'folder "{critical_folder_on_ssd_autera}" exsited.')

    list_completed_raw = get_list_completed_raw(source_folder)
    print("list_completed_raw", list_completed_raw)
    
    # split data to normal/critical
    for raw_folder_name in list_completed_raw:
        raw_folder_path = os.path.join(source_folder, raw_folder_name)
        tag_file_name = get_tag_file_name(raw_folder=raw_folder_path)
        if tag_file_name is not None:
            tag_file_path = os.path.join(raw_folder_path, tag_file_name)
            with open(tag_file_path, 'r') as file:
                content = file.read()
                if "[TAG],manual_annotation.Actuation_1,2,true" in content:
                    # Save the parent directory to the "criticalData" directory
                    destination_path = os.path.join(critical_folder_on_ssd_autera, os.path.basename(raw_folder_path))
                    shutil.move(raw_folder_path, destination_path)
                    print(f"Save the parent folder '{raw_folder_path}' to 'criticalData'")
                else:
                    print(f"this folder is not critical: {raw_folder_path}")
        print("------")

    
    # sync data
    for sync_folder_name in list_completed_raw + ['criticalData']:
        source_sync_path = os.path.join(source_folder, sync_folder_name)
        logging.info(f"sync data from {source_sync_path} to {dst_external_ssd_folder_name}")
        print('rsync -azvh --no-o --no-g --no-compress --no-perms --progress {} {}'.format(source_sync_path, dst_external_ssd_folder_name))
        os.system('rsync -azvh --no-o --no-g --no-compress --no-perms --progress {} {}'.format(source_sync_path, dst_external_ssd_folder_name))        


def other_process_running():
    if os.path.exists(LOCK_FILE):
        return True
    return False

def mark_there_is_running_process():
    with open(LOCK_FILE, 'w') as f:
        f.write('i am running')

def mark_there_is_no_process_running():
    os.remove(LOCK_FILE)


def get_car_data_folder():
    # Loop through each folder and check if the folder name contains 'VF'
    for folder_name in os.listdir(SSD_MOUNT_PATH):
        if 'VF' in folder_name:
            car_data_folder = folder_name
            break
    return car_data_folder


def main():
    logging.info('start sync data')
    if other_process_running():
        logging.info('exit because other process is running')
        sys.exit()
    else:
        mark_there_is_running_process()
    
    try:
        car_data_folder = get_car_data_folder()
        # path to folder "source_folder"
        source_folder = os.path.join(SSD_MOUNT_PATH, car_data_folder)
        logging.info("source data folder: ", source_folder)


        # Path to folder "critical"
        critical_folder_on_ssd_autera = os.path.join(source_folder, 'criticalData')
        logging.info("critical_folder_on_ssd_autera", critical_folder_on_ssd_autera)


        DEFAULT_MOUNT_POINT = '/media/autera-admin/'
        # DEFAULT_MOUNT_POINT = '/media/external_ssd/'

        dst_external_ssd_name = os.listdir(DEFAULT_MOUNT_POINT)[0]
        dst_external_ssd_folder_name = os.path.join(DEFAULT_MOUNT_POINT, dst_external_ssd_name, 'BlockBlob')        
        Path(dst_external_ssd_folder_name).mkdir(exist_ok=True, parents=True)
        move_parent_folder_of_txt_to_critical(source_folder, critical_folder_on_ssd_autera, dst_external_ssd_folder_name)
    except Exception as ex:
        logging.error("error sync data", exc_info=True)
    finally:
        logging.info("delete LOCK_FILE")
        mark_there_is_no_process_running()
        pass


main()