#!/usr/bin/env python3

import argparse
import os
import sys
import csv


def main():
	#use argparse to append every action to a list
	paser = argparse.ArgumentParser()
	
	paser.add_argument('--input', dest = 'input', required = True ,help = 'input file')
	paser.add_argument('--top', nargs = 2,dest = 'top', help='top k')
	paser.add_argument('--min', nargs='*', action='append', dest='min', help= 'compute min of given numeric field name')
	paser.add_argument('--max', nargs='*', action='append', dest='max', help= 'compute max of given numeric field name')
	paser.add_argument('--mean', nargs='*', action='append', dest='mean', help= 'compute mean of given numeric field name')
	paser.add_argument('--sum', nargs='*', action='append', dest='sum', help= 'compute min sum of given numeric field name')
	paser.add_argument('--count', action = 'store_true', help= 'compute total records')
	paser.add_argument('--group-by', action='append', dest = 'group')
	
	args = paser.parse_args()
	
	fields_min = args.min
	fields_max = args.max
	fields_mean = args.mean
	fields_sum = args.sum
	fields_count = args.count
	field_group = args.group
	fields_top = args.top
	
	
	groupby_name_li = []
	input_csv = csv.DictReader(open(sys.argv[2], encoding = 'utf-8-sig'))
	dictionary = {}
	output_headers = []
	
	#two cases in general, first case with group-by 
	if field_group != None:
		
		group_header = field_group[0]
		group_name = group_header.lower()
		
		
		#put unique group-by names to a list
		try:
			for row in input_csv:
				row = {k.lower() : v for k, v in row.items() }
				if row[group_name] not in groupby_name_li:
					groupby_name_li.append(row[group_name])
					if len(groupby_name_li) >20:
						try:
							raise OverflowError()
							
						except OverflowError:
							sys.stderr.write("Error: %s : %s has been capped at 20 distinct values" % (sys.argv[2], group_name))
							exit()
								
								
		except KeyError:
			sys.stderr.write("Error: %s :no group-by argument with %s found" % (sys.argv[2], group_header))
			exit(9)
					
		output_headers.append(group_name)
			
			
		cmin = 0
		cmax = 0
		cmean = 0 
		csum = 0
		# retrieve commands in order
		for opt in sys.argv:
			
			if opt == '--count':
				output_headers.append('count')
			
			if opt == '--min':
				temp = fields_min[cmin]
				for header in temp:
					output_headers.append('min_'+header)
				cmin+=1
				
			
			if opt == '--max':
				temp = fields_max[cmax]
				for header in temp:
					output_headers.append('max_'+header)
				cmax+=1
			
			if opt == '--mean':
				temp = fields_mean[cmean]
				for header in temp:
					output_headers.append('mean_'+header)
				cmean+=1
						
			if opt == '--sum':
				temp = fields_sum[csum]
				for header in temp:
					output_headers.append('sum_'+header)
				csum+=1		
							
			if opt == '--top':
				top_field = fields_top[1]
				output_headers.append('top_'+top_field )
				
		
		#print first line headers
		num_keys = len(output_headers)
		for i in range(num_keys):
			if (i+1) == num_keys:
				print(output_headers[i])
				
			else:
				print(output_headers[i],end = ',')
				
			
			
		top_list = []
		selected_top = []
		li = []
		results = []
		#if there is top k, then retrieve the unique value in top k field to a list 	
		if fields_top != None:
			top_field = fields_top[1]
			read_top = csv.DictReader(open(sys.argv[2], encoding = 'utf-8-sig'))
			for row in read_top:
				row = {k.lower() : v for k, v in row.items() }
				if row[top_field.lower()] not in top_list:
					top_list.append(row[top_field])
						
			
					 	
		unique_name = sorted(groupby_name_li)
			
		#iterate through unique names in group-by
		for name in unique_name:
				
			dictionary.update({group_header : name})
				
			selected_top.clear()
			li.clear()
			results.clear()
				
			#top k applied
			#get all top values to a list, than sort in descending than join with correct format
			if fields_top != None:
				top_field = fields_top[1]
				top_k = int(fields_top[0])
				for tops in top_list:
					selected_top.append(groupby_top(name, group_name ,tops, top_field))
					
				selected_top.sort(key = lambda x: x[1], reverse = True)
				results = selected_top[0:top_k]
				for (x,y) in results:
					temp = x+ ': '+ str(y)
					li.append(temp)
				
				str1 = ','
				t = str1.join(li)
				dictionary.update({'top_'+top_field : t})
					
					
			#count applied update dictionary			
			if fields_count:
				count_num = group_count(name,group_name)
				dictionary.update({'count' : count_num})				
				
				
			#max applied update dictionary
			if fields_max != None:
				#loop through max header
				for header_list in fields_max:
					for header in header_list:
						h2 = header.lower()
						max_num = group_max(h2, name,group_name)
						dictionary.update({'max_'+header : max_num})
							
				
			#min applied update dictionary									
			if fields_min != None:
				#loop through min header
				for header_list in fields_min:
					for header in header_list:
						h2 = header.lower()
						min_num = group_min(h2, name,group_name)
						dictionary.update({'min_'+header : min_num})
							
							
			#mean applied update dictionary										
			if fields_mean != None:
				#loop through min header
				for header_list in fields_mean:
					for header in header_list:
						h2 = header.lower()
						mean_num = group_mean(h2, name,group_name)
						dictionary.update({'mean_'+header : mean_num})
							
							
			#sum applied update dictionary									
			if fields_sum != None:
				#loop through sum header
				for header_list in fields_sum:
					for header in header_list:
						h2 = header.lower()
						sum_num = group_sum(h2, name,group_name)
						dictionary.update({'sum_'+header : sum_num})
						
			#print all values line by line		
			list_vals = []
			for j in output_headers:
				list_vals.append(dictionary[j])
			
			num_vals = len(list_vals)
			
			for x in range(num_vals):
				if (x%num_keys) == (num_vals-1):
					print (list_vals[x])
					
				else:
					print(list_vals[x], end = ',')
				
				
						
												
			
	#this is the case when there is no group-by
	else:
		input_file = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
		output_dictionary = {}
		headers = []
	
		
			
		cmin = 0
		cmax = 0
		cmean = 0 
		csum = 0
		
		# retrieve commands in order
		for opt in sys.argv:
			
			if opt == '--count':
				headers.append('count')
			
			if opt == '--min':
				temp = fields_min[cmin]
				for header in temp:
					headers.append('min_'+header)
				cmin+=1
				
			
			if opt == '--max':
				temp = fields_max[cmax]
				for header in temp:
					headers.append('max_'+header)
				cmax+=1
			
			if opt == '--mean':
				temp = fields_mean[cmean]
				for header in temp:
					headers.append('mean_'+header)
				cmean+=1
						
			if opt == '--sum':
				temp = fields_sum[csum]
				for header in temp:
					headers.append('sum_'+header)
				csum+=1		
							
			if opt == '--top':
				top_field = fields_top[1]
				headers.append('top_'+top_field )
		
		
		#print first line headers
		num_keys = len(headers)
		
		for i in range(num_keys):
			if (i+1)==num_keys:
				print(headers[i])
			else:
				print(headers[i],end=',')
		
					
		
		#retrieve needed data to dictionary				
		if fields_count:
			count_num = count_func(input_file)
			output_dictionary.update({'count' : count_num})
			
		#min applied,update values to dictionary
		if fields_min != None:
			for name_list in fields_min:
				for name in name_list:
					header_name='min_'+name
					field_name = name.lower()
					min_num = min_func(field_name)
					output_dictionary.update({header_name : min_num})
				
		#max applied update dictionary
		if fields_max != None:
			for name_list in fields_max:
				for name in name_list:
					header_name = 'max_'+name
					field_name = name.lower()
					max_num = max_func(field_name)
					output_dictionary.update({header_name : max_num})
					
		#min applied update dictionary
		if fields_mean !=None:
			for name_list in fields_mean:
				for name in name_list:
					header_name = 'mean_'+name
					field_name = name.lower()
					mean_num = mean_func(field_name)
					output_dictionary.update({header_name : mean_num})
					
		#sum applied update dictionary
		if fields_sum !=None:
			for name_list in fields_sum:
				for name in name_list:
					header_name = 'sum_'+name
					field_name = name.lower()
					sum_num = sum_func(field_name)
					output_dictionary.update({header_name : sum_num})
						
		#get all top values to a list, than sort than join
		if fields_top != None:
				
			top_list = []
			selected_top = []
			li = []
			results = []
				
			read_top = csv.DictReader(open(sys.argv[2], encoding = 'utf-8-sig'))
				
			for row in read_top:
				row = {k.lower() : v for k, v in row.items() }
				if row[top_field.lower()] not in top_list:
					top_list.append(row[top_field])
						
						
			top_field = fields_top[1]
			top_k = int(fields_top[0])
				
			for tops in top_list:
				selected_top.append(tk(tops, top_field))
				
			selected_top.sort(key = lambda x: x[1], reverse = True)
			results = selected_top[0:top_k]
			for (x,y) in results:
				temp = x+ ': '+ str(y)
				li.append(temp)
				
			str1 = ','
			t = str1.join(li)
				
			output_dictionary.update({'top_'+top_field : t})
		

		
		list_values = []
		for j in headers:
			list_values.append(output_dictionary[j])
			
		num_values = len(list_values)
			
		for k in range(num_values):
			if (k%num_keys) == (num_keys-1):
				print(list_values[k])
					
			else:
				print(list_values[k], end = ',')
				
				
	
	
""" function for max"""
def max_func (max_name):
	
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	entry_list = []
	#loop through file and find corresponding data with given field
	try:
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			try:
				entry_list.append(float(row[max_name]))
				
			except ValueError:
				sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],max_name))
				exit()
				
			
	except KeyError:
		sys.stderr.write("Error: %s :no field with name %s found" % (sys.argv[2], max_name))
		exit(8)
	
	maxnum = max(entry_list)
	entry_list.clear()
	
	return maxnum
	
	
"""function for min"""	
def min_func (min_name):
	
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	entry_list = []
	#loop through file and find corresponding data with given field	
	try:	
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			try:
				entry_list.append(float(row[min_name]))
				
			except ValueError:
				sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],min_name))
				exit()
				
				
	
	except KeyError:
		sys.stderr.write("Error: %s :no field with name %s found" % (sys.argv[2], min_name))
		exit(8)		
			
			
	minnum = min(entry_list)
	entry_list.clear()
	
	return minnum
	

		
"""function for mean"""		
def mean_func (mean_name):
	
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	entry_list = []
	#loop through file and find corresponding data with given field
	try:
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			try:
				entry_list.append(float(row[mean_name]))
				
			except ValueError:
				sys.stderr.write( "Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],mean_name))
				exit()
	
	
	except KeyError:
		sys.stderr.write( "Error: %s :no field with name %s found" % (sys.argv[2], mean_name))
		exit(8)
				
	meannum = sum(entry_list)/len(entry_list)
	entry_list.clear()
	
	return meannum

"""function for sum"""	
def sum_func (sum_name):
	
	
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	entry_list = []
	#loop through file and find corresponding data with given field
	try:	
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			try:
				entry_list.append(float(row[sum_name]))
				
			except ValueError:
				sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],sum_name))
				exit()
			
	except KeyError:
		sys.stderr.write("Error: %s :no field with name %s found" % (sys.argv[2], sum_name))
		exit(8)
		
				
	total = sum(entry_list)
	entry_list.clear()
	
	return total


"""function for count"""
def count_func(input_csv):
	
	num_row = 0
	
	for row in input_csv:
		num_row+=1
		
	return num_row
	
"""function for top k"""
def tk (top_name, top_field):
	
	repeats = 0
	tu = ()
	#loop through file and find corresponding data with given field, than append to tuple
	try:
		input_csv = csv.DictReader(open('input.csv',encoding='utf-8-sig'))
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if top_name == row[top_field]:
				repeats += 1
	except KeyError:
		sys.stderr.write ("Error: %s :no field with name %s found" % (sys.argv[2], top_field))
		exit(8)
		
	tu += (top_name, repeats)
	
	return tu	


"""group-by max function"""
def group_max (h , n, fn):
	
	max_list=[]
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	
	try:
		#if the name in group by == row[group_by field]
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if n == row[fn]:
				try:
					max_list.append(float(row[h]))
					
				except ValueError:
					sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],h))
					exit()
				
	except KeyError:
		sys.stderr.write("Error: %s :no field with name %s found" % (sys.argv[2], h))
		exit(8)
			
	maxn = max(max_list)
	max_list.clear()
	
	return maxn
	
	
"""group-by min function"""
def group_min (h , n, fn):
	
	min_list=[]
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	#if the name in group by == row[group_by field]
	
	try:
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if n == row[fn]:
				try:
					min_list.append(float(row[h]))
					
				except ValueError:
					sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],h))
					exit()
			
	except KeyError:
		sys.stderr.write("Error: %s :no field with name %s found" % (sys.argv[2], h))
		exit(8)
			
	minnum = min(min_list)
	min_list.clear()
	
	return minnum
	
	
"""group-by mean function"""
def group_mean (h , n, fn):
	
	mean_list=[]
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	
	try:
		
		#if the name in group by == row[group_by field]
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if n == row[fn]:
				try:
					mean_list.append(float(row[h]))
					
				except ValueError:
					sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],h))
					exit()
			
	except KeyError:
		sys.stderr.write ("Error: %s :no field with name %s found" % (sys.argv[2], h))
		exit(8)
			
	meannum= sum(mean_list)/len(mean_list)
	mean_list.clear()
	
	return meannum
		
	
"""group-by sum function"""	
def group_sum (h , n, fn):
	
	sum_list=[]
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	
	try:	
		#if the name in group by == row[group_by field]
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if n == row[fn]:
				try:
					sum_list.append(float(row[h]))
				
				except ValueError:
					sys.stderr.write("Error: %s:can't compute %s on non-numeric value" % (sys.argv[2],h))
					exit()
	
	except KeyError:
		sys.stderr.write ("Error: %s :no field with name %s found" % (sys.argv[2], h))
		exit(8)
				
	total = sum(sum_list)
	sum_list.clear()
	
	return total
	
	
"""group-by count"""
def group_count (n, fn):
	
	input_csv = csv.DictReader(open(sys.argv[2],encoding='utf-8-sig'))
	c = 0
		
	#if the name in group by == row[group_by field]
	for row in input_csv:
		row = {k.lower() : v for k, v in row.items() }
		if n == row[fn]:
			c+=1
			
	return c

"""group-by top """
def groupby_top(groupby_name, groupby_field,top_name,top_field):
	
	repeats = 0
	tu = ()
	#if find name with corresponding group field and top field, than append top name and value to tuple 
	try:
		input_csv = csv.DictReader(open('input.csv',encoding='utf-8-sig'))
		for row in input_csv:
			row = {k.lower() : v for k, v in row.items() }
			if top_name == row[top_field] and groupby_name == row[groupby_field]:
				repeats += 1
	
	except KeyError:
		sys.stderr.write ("Error: %s :no field with name %s found" % (sys.argv[2], top_field))
		exit(8)
		
	tu += (top_name, repeats)
	
	return tu
			
		
if __name__ == '__main__':
    main()

