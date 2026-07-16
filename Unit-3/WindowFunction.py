from pyflink.datastream.functions import WindowFunction


class SummaryWindow(WindowFunction):
	def apply(self, key, window, inputs):
		items = list(inputs)
		count = len(items)
		total = sum(x[1] for x in items)

		return [(
			key,
			count,
			total,
		)]