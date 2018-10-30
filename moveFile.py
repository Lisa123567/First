import os, time, datetime

# Gets today time/date 
today = datetime.date.today()
today = str(today)

print("Today's Date:", today, "\n\n\n")

logFolder = "C:/Users/Larisa Vasukov/Anaconda2/LogFolder"
logFolderContent = os.listdir(logFolder) 
print("log folder content", logFolderContent)
allFilesCreatedToday = True
allFilesSizeNotZero = True
allFilesHaveErrorString = True
report_file = open('C:/Users/Larisa Vasukov/Anaconda2/report%s.txt' %(today),'w+')

for filename in logFolderContent:
    
	file = os.path.join(logFolder, filename)
	fileCreationTime = time.strftime('%Y-%m-%d', time.gmtime(os.path.getctime(file)))
	print 'File %s created on %s' %(file,fileCreationTime)
	
	# All files are created today
	if fileCreationTime == today:
		print 'File %s created today' %(file)
		report_file.write('File %s created today \n' %(file))
	else:
		print 'File %s NOT created today' %(file)
		report_file.write('File %s NOT created today \n' %(file))
		allFilesCreatedToday = False
		
	# Check that all logs size 	
	if (os.path.getsize(file) > 0):
		print 'File %s size is GREATER then zero' %(file)
		report_file.write('File %s size is GREATER then zero \n' %(file))
	else:
		print 'File %s size is LESS then zero' %(file)
		report_file.write('File %s size is LESS then zero \n' %(file))
		allFilesSizeNotZero = False
		
	# Check for "Error" string in file
	logFile = open(file)
	if 'Error' in logFile.read():
		print 'File %s has ERROR in it' %(file)
		report_file.write('File %s size has ERROR in it \n' %(file))
	else:
		print 'File %s has no ERROR in it' %(file)
		report_file.write('File %s has no ERROR in it \n' %(file))
		allFilesHaveErrorString = False
	
	logFile.close()
    
report_file.close()
	