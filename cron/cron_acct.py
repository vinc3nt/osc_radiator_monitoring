#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import csv
import tempfile
import datetime
import sys
import gzip
from collections import defaultdict
import shutil
import time
import ConfigParser


# Config file
Config = ConfigParser.ConfigParser()
Config.read('cron.cfg')

def ConfigSectionMap(section):
   """ ritorna il valore della sezione di configurazione
       specificata """
   dict1 = {}
   options = Config.options(section)
   for option in options:
      try:
         dict1[option] = Config.get(section, option)
         if dict1[option] == -1:
            DebugPrint("skip: %s" % option)
      except:
         print("exception on %s!" % option)
         dict1[option] = None
   return dict1

def set_path(debug):
   """Crea una directory temporanea"""
   global path
   path = tempfile.mkdtemp()
   if debug:
      print "[+] Creata directory: " + path
   return path

def yesterday():
   """Ritorna il giorno di ieri in formato YYYYMMDD"""
   today = datetime.date.today()
   yesterday = today - datetime.timedelta(1)
   return yesterday.strftime("%Y%m%d")

def logfilename():
   """ Ritorna il path ed il filename del file di log di accounting
    da analizzare """
    return "%s-" + yesterday() + ".gz" % (ConfigSectionMap("log")['acct'])

def correlate_acct():
   """ Richiamo la lo script perl analyze_acct.pl che permette
   di correlare tra loro le date di start e stop per ogni matricola """
   subprocess.Popen('zcat %s | perl analize_acct.pl > %s/acct.csv' % (logfilename(), path), shell=True)
   return "%s/acct_epoch.csv" % (path)

def custom_acct(debug):
   """ Converte il csv con il timestamp in formato epoch e crea un nuovo csv gizippato"""
   try:
      with open('%s', 'r+') % (correlate_acct()) as f:
         csvlog = csv.reader(f, delimiter=';')
         csvpath = '%s/acct-%s_%s.csv.gz' % (path, yesterday(), ConfigSectionMap("client")['hostname']))
         f = gzip.open(csvpath, 'w')
         for row in csvlog:
            oldstart = row[1]
            oldstop = row[2]
            start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(oldstart)))
            stop = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(oldstop)))
            f.write(row[0]+";"+start+";"+stop+"\n")
         f.close()
      if debug:
         print "[+] Creato il csv con i dati di accounting"
   except:
      print "[-] Errore nella generazione dei dati di accounting"

def scp_copy(csvpath, user, host, srvpath):
   return not subprocess.Popen(["scp", csvpath, "%s@%s:%s" % (user, host, srvpath)]).wait()

def clean_tmp(debug):
   try:
      shutil.rmtree(path)
      if debug:
         print "[+] Rimossa la directory temporanea:" + path
   except OSError, e:
      if e.errno != 2:
         raise

def main():
   debug = int(ConfigSectionMap("debug")['debug'])
   set_path(debug)
   try:
      custom_acct(debug)
      csvpath = '%s/acct-%s_%s.csv.gz' % (path, yesterday(), ConfigSectionMap("client")['hostname']))
      if scp_copy(csvpath, ConfigSectionMap("ssh")['user'], ConfigSectionMap("ssh")['host'], ConfigSectionMap("ssh")['path']):
         if debug:
            print "[+] File uploaded successfully"
         return 0
      else:
         if debug:
            print "[-] File upload failed"
         return 1
   except IOError:
      if debug:
         print "[-] Il file " + logfilename() + " non e' presente"
   clean_tmp(debug)
   return 0

if __name__ == '__main__':
   main()
