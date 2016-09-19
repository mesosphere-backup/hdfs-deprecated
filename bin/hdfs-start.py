import sys
from optparse import OptionParser
from subprocess import STDOUT, check_call
import subprocess
import shutil
import xml.etree.ElementTree as ET

def parse_args():
  parser = OptionParser(usage="[options] [property=value]* ")
  parser.add_option("--mesos.conf.path",  dest="cnfpath", default="none", help="config file path to configure before start")
  (opts, args) = parser.parse_args()
  return (opts, args)

def findProperty(xmlElement, propertyName) :
  for prop in xmlElement.findall("property"):
     if(prop.find("name").text == propertyName):
        return prop
  return None;
     

def appendProperty(xmlElement, propertyName, description, value):
  propertyElement = findProperty(xmlElement, propertyName);
  if(None == propertyElement):
      propertyElement = ET.SubElement(xmlElement, "property")
      ET.SubElement(propertyElement, "name")
      ET.SubElement(propertyElement, "description")
      ET.SubElement(propertyElement, "value")  
  propertyElement.find("name").text = propertyName
  if(description):
     propertyElement.find("description").text = description
  propertyElement.find("value").text = value;
  
  

def configure(opts, args):
  cnfPath = opts.cnfpath
  tree = ET.parse(cnfPath)
  configElement = tree.getroot()
  for a in args:
     (name, value) = a.split('=')
     appendProperty(configElement, name, "", value)
  tree.write(cnfPath, xml_declaration=True,encoding='utf-8',
           method="xml")
  

if __name__ == "__main__":
  (opts, args) = parse_args()
  configure(opts, args)
  
  cnfPath = opts.cnfpath
  f = open(cnfPath, 'r')
  print f.read()
  subprocess.call("./bin/hdfs-mesos", shell=True)

  
