import re
import os

arff = open('data.arff', 'w+')

try:
	# Arff header
	arff.write('@RELATION amazonemotion\n')
	arff.write('@ATTRIBUTE review\tSTRING\n')
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
	arff.write('@ATTRIBUTE usefulvotes\tNUMERIC\n')
	arff.write('@ATTRIBUTE totalvotes\tNUMERIC\n')
	arff.write('@ATTRIBUTE usefulness\tNUMERIC\n')
	arff.write('@ATTRIBUTE class\t{1,1.5,2,2.5,3,3.5,4,4.5,5}\n')
	arff.write('@DATA\n')

	# Find output files to read from
	partfiles = re.compile('^part-r-', re.IGNORECASE)   
	files = [file for file in os.listdir('.') if partfiles.search(file)]

	# Read each file in
	for i,file in enumerate(files):
		filesize  = os.path.getsize(file)
		raw = open(file, 'r')
		try:
			for j,line in enumerate(raw):
				# Print only every 10k lines to reduce stdout usage
				if j % 10000 == 0:
					print '%(percent).3f of %(curfile)i/%(numfiles)i file(s)' % \
						{'percent' : float(raw.tell()) / filesize,
						'curfile' : i + 1,
						'numfiles' : len(files)}
				line = [part.strip() for part in line.split('\t')]
				# Determine the percentage of people who found a review helpful (0 if no votes were cast)
				if int(line[15]) > 0:
					usefulness = '%.3f' % (float(line[14]) / float(line[15]))
				else:
					usefulness = '0'
				# Rearrange line to values to match the header
				line = line[0:13] + line[14:16] + [usefulness]+ [line[13]]
				arff.write('%s\n' % ','.join(line))
		finally:
			raw.close()
finally:
	arff.close()
