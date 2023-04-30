#!/usr/bin/env python3

import os
from PIL import Image
from pdf2image import convert_from_path
import pytesseract
import yaml
import re
import sys
import datetime
import json
import dateparser
from pdf2image.exceptions import *
import pwd
import tempfile
import glob
import time
import math
import signal

class PDFGrep:

  def __init__(self):
    self.home_dir = pwd.getpwuid(os.getuid()).pw_dir
    self.database_path = self.home_dir + '/.config/pdfgrep'

    if not os.path.exists(self.home_dir + '/.config'):
      os.mkdir(self.home_dir + '/.config', 0o700)

    if not os.path.exists(self.database_path):
      os.mkdir(self.database_path, 0o700)

    self.database_file = self.database_path + '/database.db'

    if os.path.exists(self.database_file):
      try:
        self.database = json.loads(open(self.database_file).read())
      except Exception as e:
        print("failed to load database file: %s - %s" % (self.database_file, str(e)))
        sys.exit(1)
    else:
      self.database = {}

    self.tmppath = tempfile.mkdtemp(prefix='/tmp/')

    signal.signal(signal.SIGINT, self.handler)


  def main(self):
    cleanup = False
    index = False
    index_paths = []
    search = []
    ignore = []
    skip = False

    for i in range(1, len(sys.argv)):
      if skip:
        skip = False
        continue

      if sys.argv[i] == '-c':
        cleanup = True
        continue
      if sys.argv[i] == '-i':
        index = True
        continue
      if sys.argv[i] == '--ignore':
        ignore.append(sys.argv[i+1])
        skip = True
        continue

      if index:
        index_paths.append(sys.argv[i])
      else:
        search.append(sys.argv[i])

    if index and len(index_paths) >0:
      self.index(index_paths, ignore, cleanup)
      sys.exit()
    elif len(search) >0:
      self.search(search)
      sys.exit()

    self.usage()


  def usage(self):
    print("\nusage:\n")
    print("%s -i [-c] [--ignore <path>] [--ignore <path] <path>   : index pdfs at path" % (sys.argv[0].split('/')[-1]))
    print("-c will drop database entries for files at <path> no longer on disk or in ignored paths\n")
    print("%s <string | regex>                                    : grep pdfs\n" % (sys.argv[0].split('/')[-1]))


  def index(self, paths_to_index, paths_to_ignore, cleanup):
    pdf_files = []

    start = time.time()

    for index_path in paths_to_index:
      for path in glob.glob('%s/**/*' % (index_path), recursive=True):
        if path.split('.')[-1].lower() != 'pdf':
          continue

        path = os.path.abspath(path)

        if not os.path.exists(path):
          continue

        ignore = False

        for path_to_ignore in paths_to_ignore:
          if path[0:len(path_to_ignore)] == path_to_ignore:
            ignore = True
            break

        if not ignore:
          pdf_files.append(path)

    workload, total_todo = self.build_workload(pdf_files)

    self.pids = {}

    for thread_id in workload:
      pid = os.fork()

      if pid == 0:
        self.workload_thread(thread_id, workload[thread_id])
        sys.exit()
      else:
        self.pids[thread_id] = pid

    self.pdfs_done = 0
    last_done = 0

    while 1:
      threads_complete = True

      for thread_id in self.pids:
        pid = self.pids[thread_id]
        try:
          status, rc = os.waitpid(pid, 1)
        except ChildProcessError:
          continue

        if status == 0:
          threads_complete = False

        self.process_results(thread_id)

      if threads_complete:
        break

      time.sleep(0.5)

      elapsed = time.time() - start
      if self.pdfs_done > 0:
        per_item = elapsed / self.pdfs_done
      else:
        per_item = 0

      time_remaining = per_item * (total_todo - self.pdfs_done)
      percent_done = self.pdfs_done / (total_todo / 100)

      if self.pdfs_done > last_done:
        print("processed %d/%d - %.2f%% [%.2f/min ETA %s]" % (self.pdfs_done, total_todo, percent_done, 60 / per_item, self.to_time_string(time_remaining)))

      last_done = self.pdfs_done

    if cleanup:
      self.cleanup(paths_to_index, pdf_files, paths_to_ignore)


  def build_workload(self, pdf_files):
    threads = int(os.popen("/usr/sbin/sysctl -n hw.ncpu").read().rstrip()) / 2
    thread_id = 0
    workload = {}
    total_todo = 0

    for path in pdf_files:
      mtime = int(os.stat(path).st_mtime)
      fsize = os.stat(path).st_size

      if path in self.database and self.database[path]['mtime'] == mtime and self.database[path]['fsize'] == fsize:
        continue

      if thread_id not in workload:
        workload[thread_id] = []

      workload[thread_id].append({
        'path': path,
        'mtime': mtime,
        'fsize': fsize
      })

      thread_id += 1
      total_todo += 1

      if thread_id >= threads:
        thread_id = 0

    return workload, total_todo


  def workload_thread(self, thread_id, workload):
    tmppath = self.tmppath + '/' + str(thread_id)

    os.mkdir(tmppath, 0o700)

    i = 0

    for item in workload:
      try:
        doc = convert_from_path(item['path'])
      except PDFPageCountError as e:
        if 'Incorrect password' in str(e):
          sys.stdout.write("cannot index: %s - password protected\n" % (item['path']))
          sys.stdout.flush()
        else:
          sys.stdout.write("cannot index: %s - %s\n" % (item['path'], str(e)))
          sys.stdout.flush()

        continue

      text = []

      for page_number, page_data in enumerate(doc):
        sys.stdout.write("indexing: %s page %d ...\n" % (item['path'], page_number + 1))
        sys.stdout.flush()

        txt = pytesseract.image_to_string(page_data)
        text.append(txt)

      item_file = tmppath + '/%d.tmp' % (i)

      with open(item_file, 'w') as f:
        f.write(json.dumps({
          'path': item['path'],
          'mtime': item['mtime'],
          'fsize': item['fsize'],
          'pages': text
        }))

      os.rename(tmppath + '/%d.tmp' % (i), tmppath + '/%d' % (i))

      i += 1


  def save(self):
    with open(self.database_file + '.new', 'w') as f:
      f.write(json.dumps(self.database))

    os.rename(self.database_file + '.new', self.database_file)


  def process_results(self, thread_id):
    for path in glob.glob('%s/%d/*' % (self.tmppath, thread_id)):
      if path.split('/')[-1].isdigit():
        obj = json.loads(open(path).read())

        self.database[obj['path']] = {
          'mtime': obj['mtime'],
          'fsize': obj['fsize'],
          'pages': obj['pages']
        }

        self.save()
        self.pdfs_done += 1

        os.remove(path)


  def to_time_string(self, time_remaining):
    time_string = ''

    if time_remaining >= 86400:
      days = int(math.floor(time_remaining / 86400))
      time_remaining -= (days * 86400)

      time_string += '%d days, ' % (days)

    hours = 0
    mins = 0

    if time_remaining >= 3600:
      hours = int(math.floor(time_remaining / 3600))
      time_remaining -= (hours * 3600)

    if time_remaining >= 60:
      mins = int(math.floor(time_remaining / 60))
      time_remaining -= (mins * 60)

    time_string += '%s:%s:%s' % (
      str(hours).rjust(2,'0'),
      str(mins).rjust(2,'0'),
      str(int(time_remaining)).rjust(2,'0')
    )

    return time_string


  def handler(self, signum, frame):
    for pid in self.pids:
      os.kill(pid, signal.SIGTERM)


  def search(self, search_string):
    search_string = ' '.join(search_string)

    for path in sorted(self.database):
      pagen = 0
      for page in self.database[path]['pages']:
        i = 0
        for line in page.split("\n"):
          if search_string.lower() in line.lower():
            print("%s: %d:%d - %s" % (path, pagen+1, i, line))

          i += 1

        pagen += 1


  def cleanup(self, paths_to_index, pdf_files, paths_to_ignore):
    to_remove = []

    for path in self.database:
      if not self.path_in_paths(path, paths_to_index):
        continue

      if path not in pdf_files:
        print("cleanup, file deleted: %s" % (path))
        to_remove.append(path)
        continue

      if self.path_in_paths(path, paths_to_ignore):
        print("cleanup, path ignored: %s" % (path))
        to_remove.append(path)

    for path in to_remove:
      self.database.pop(path)

    self.save()


  def path_in_paths(self, path, path_list):
    for item in path_list:
      if path[0:len(item)] == item:
        return True

    return False


p = PDFGrep()
p.main()
