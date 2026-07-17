from pyflink.datastream.functions import WindowFunction

class SummaryWindow(WindowFunction):
	def apply(self, key, window, inputs):
		items = list(inputs)
		count = len(items)
		total = sum(x[1] for x in items)

		return [(key,count,total)]
	


from pylink.datastream.functions import Windowfunction

class SummaryWindow(WindowFunction):
	def apply(self,key,input):
		items=list(input)
		count=len(items)
		total=sum(x[1] for x in items)

		return [(key,count,total)]
	





from pyflink.datastream.functions import WindowFunction

class SummaryWindow(WindowFunction):
	def apply(self, input, key, count, total):
		items=list(input)
		count=len(items)
		total=sum(x[1] for x in items)

		return [(key,count,total)]




from pyflink.datastream.functions import WindowFunction

class SummaryWindow(WindowFunction):
	def apply(self,key,input):
		items=list(input)
		count=len(items)
		total=sum(x[0] for x in items)

		return [(key,count,total)]

				  