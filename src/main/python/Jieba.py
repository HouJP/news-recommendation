# encoding=utf-8
import jieba
import sys

# segment `line` by jieba
def segment_line(line):
	sub_lines = line.split('\t', 1)
	nid = sub_lines[0]
	content = sub_lines[1]
	words = jieba.cut(content.decode("utf8"))
	words = filter_words(words)
	return nid + "\t" + " ".join(words).encode("utf8")

def filter_words(words):
	words = [word for word in words if len(word) > 1]
	return words

if __name__ == "__main__":
	nid_content = ""
	line = sys.stdin.readline()
	while line:
		if -1 != line.find("\t"):
			if "" != nid_content:
				print segment_line(nid_content)
			nid_content = line
		else:
			nid_content += " " + line
		line = sys.stdin.readline()
	if "" != nid_content:
		print segment_line(nid_content)
		
