import re
import os

rawfile = 'part-r-00000'

# Open files to read/write
arff = open('data.arff', 'w+')
vals = open('vals', 'w+')	# Output the total sums when found
raw = open(rawfile, 'r')

try:
	# Arff header
	arff.write('@RELATION amazonemotion\n')
	arff.write('@ATTRIBUTE anger\tNUMERIC\n')
	arff.write('@ATTRIBUTE anticipation\tNUMERIC\n')
	arff.write('@ATTRIBUTE disgust\tNUMERIC\n')
	arff.write('@ATTRIBUTE fear\tNUMERIC\n')
	arff.write('@ATTRIBUTE joy\tNUMERIC\n')
	arff.write('@ATTRIBUTE sadness\tNUMERIC\n')
	arff.write('@ATTRIBUTE surprise\tNUMERIC\n')
	arff.write('@ATTRIBUTE trust\tNUMERIC\n')
	arff.write('@ATTRIBUTE noEmotion\tNUMERIC\n')
	arff.write('@ATTRIBUTE positive\tNUMERIC\n')
	arff.write('@ATTRIBUTE negative\tNUMERIC\n')
	arff.write('@ATTRIBUTE noSentiment\tNUMERIC\n')
	arff.write('@ATTRIBUTE class\t{1.0,1.5,2.0,2.5,3.0,3.5,4.0,4.5,5.0}\n')
	arff.write('@DATA\n')
	
	# Read file in
	filesize  = os.path.getsize(rawfile)
	for i,line in enumerate(raw):
		# Print only every 10k lines to reduce stdout usage
		if i % 10000 == 0:
			print '%(percent).3f of %(curfile)i/%(numfiles)i file(s)' % \
						{'percent' : float(raw.tell()) / filesize,
						'curfile' : 1,
						'numfiles' : 1}
		# If the line contains a total, output that
		if 'anger' in line:
			vals.write(line.strip() + '\n')
		# Else 
		else:
			line = [part.strip() for part in line.split('\t')]
			# Rearrange line to values to match the header
			line = line[1:] + [line[0]]
			arff.write('%s\n' % ','.join(line))
finally:
	arff.close()
	raw.close()
	vals.close()
