##Author: Rene Pfitzner,
##August 2013
##Updates: Vahan Nanumyan

#This module implements scientometrics multiplex structures using graph_tool

import graph_tool.all as gt
import csv
import itertools
import random
import numpy
import time
import pickle
import copy
import zipfile
import os 
import sys
import datetime
from dateutil import parser
import psycopg2


class PaperAuthorMultiplex():
    'Paper Citation and Author Collaboration Multiplex Structure'

#############################################################
    #Initialize empty object
    def __init__(self):
        
        #create empty multiplex structure
        self.collab = gt.Graph(directed=False)
        self.citation = gt.Graph(directed=True)

        self.citation.vertex_properties['year']=self.citation.new_vertex_property('object')
        self.citation.vertex_properties['_graphml_vertex_id']=self.citation.new_vertex_property('string')
        self.citation.edge_properties['year']=self.citation.new_edge_property('object')
        
        self.collab.vertex_properties['year']=self.collab.new_vertex_property('object')
        self.collab.vertex_properties['_graphml_vertex_id']=self.collab.new_vertex_property('string')
        self.collab.edge_properties['first_year_collaborated']=self.collab.new_edge_property('object')

        self._multiplex_collab = self.collab.new_vertex_property('object')
        self._multiplex_citation = self.citation.new_vertex_property('object')
        
        self._collab_graphml_vertex_id_to_gt_id = {}
        self._citation_graphml_vertex_id_to_gt_id = {}

        # to turn the collaboration network into weighted:
        self.collab.edge_properties['weight']=self.collab.new_edge_property('int')
        self.collab.edge_properties['dates']=self.collab.new_edge_property('object')
        
        
    
################################################################
    ##
    #Function to add new papers, incl. collaborations
    def add_paper(self,paper_id,year,author_list,update_collaborations=True):
        '''
        Add a paper with paper_id (str), publication year (any standard date format) and authors specified in author_list (list<str>) to the multiplex.
        Collaborations are automatically updated, unless otherwise specified.
        VN: the function 
        '''
        
        #try whether paper exists already in citation network
        try:
            self._citation_graphml_vertex_id_to_gt_id[paper_id]
            raise PaperIDExistsAlreadyError() #stop execution here with this error
        except KeyError:
            pass
        
        #add new paper to citation network and additional data structures
        new_paper=self.citation.add_vertex()
        self._citation_graphml_vertex_id_to_gt_id[paper_id]=self.citation.vertex_index[new_paper]
        self.citation.vertex_properties['_graphml_vertex_id'][new_paper]=paper_id
        self.citation.vertex_properties['year'][new_paper]=parse_date(year)
        self._multiplex_citation[new_paper]={}
        
        
        #add collaborations between authors on collab network
        if update_collaborations == True:
            if len(author_list) == 1:
                self.add_collaboration(author_list[0], author_list[0], year, new_paper)
                
            #add collaborations, if older, registered collaborations do not exist
            for author_comb in itertools.combinations(author_list,2):
                self.add_collaboration(author_comb[0], author_comb[0], year, new_paper)
                
        return new_paper

################################################################
    ##
    #Funtion to add multiplex interconnection
    def add_multiplex(self,paper_id,author_id,year):
        try:
            new_paper=self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id])
        except KeyError:
            new_paper=self.citation.add_vertex()
            self._citation_graphml_vertex_id_to_gt_id[paper_id]=self.citation.vertex_index[new_paper]
            self.citation.vertex_properties['_graphml_vertex_id'][new_paper]=paper_id
            self.citation.vertex_properties['year'][new_paper]=parse_date(year)
            self._multiplex_citation[new_paper]={}            
        
        try:
            new_author=self.collab.vertex(self._collab_graphml_vertex_id_to_gt_id[author_id])
        except KeyError:
            new_author = self.collab.add_vertex()
            self._collab_graphml_vertex_id_to_gt_id[author_id]=self.collab.vertex_index[new_author]
            self.collab.vertex_properties['year'][new_author]=parse_date(year)
            self.collab.vertex_properties['_graphml_vertex_id'][new_author]=author_id
            self._multiplex_collab[new_author]={}
        
        #add multiplex information
        self._multiplex_collab[new_author][new_paper]=True
        self._multiplex_citation[new_paper][new_author]=True

        


################################################################
    ##
    #Funtion to add citation to citation network
    def add_citation(self,cited_paper,citing_paper):
        '''Add citation between two paper in citation network.'''
        try:
            cited_paper_gt=self._citation_graphml_vertex_id_to_gt_id[cited_paper]
        except KeyError:
            raise NoSuchPaperError()
            
        try:
            citing_paper_gt=self._citation_graphml_vertex_id_to_gt_id[citing_paper]
        except KeyError:
            raise NoSuchPaperError()

        if self.citation.edge(cited_paper_gt,citing_paper_gt)==None:
            new_citation=self.citation.add_edge(cited_paper_gt,citing_paper_gt)
            self.citation.edge_properties['year'][new_citation]=self.citation.vertex_properties['year'][self.citation.vertex(citing_paper_gt)]
        else:
            raise CitationExistsAlreadyError()
                 

################################################################    
    ##
    #Function to add plain new collaboration, independent of papers, from other sources
    def add_collaboration(self,author1, author2, year,vpaper=None):
        '''
        Add collaboration between two authors
        if provided `vpaper` (citations vertex), updates mutiplex structure
        '''
        
        if author1==author2: #simply add the author to the network, if not existing
            try:
                new_author=self._collab_graphml_vertex_id_to_gt_id[author1]
            except KeyError:
                new_author = self.collab.add_vertex()
                self._collab_graphml_vertex_id_to_gt_id[author]=self.collab.vertex_index[new_author]
                self.collab.vertex_properties['year'][new_author]=parse_date(year)
                self.collab.vertex_properties['_graphml_vertex_id'][new_author]=author1
                self._multiplex_collab[new_author]={}    

            if vpaper:
                self._multiplex_collab[new_author][vpaper]=True
                self._multiplex_citation[vpaper][new_author]=True
            
        else: 
            for author in [author1,author2]:
                try:
                    new_author=self._collab_graphml_vertex_id_to_gt_id[author]
                except KeyError:
                    new_author = self.collab.add_vertex()
                    self._collab_graphml_vertex_id_to_gt_id[author]=self.collab.vertex_index[new_author]
                    self.collab.vertex_properties['year'][new_author]=parse_date(year)
                    self.collab.vertex_properties['_graphml_vertex_id'][new_author]=author
                    self._multiplex_collab[new_author]={}
                    
            if vpaper:
                self._multiplex_collab[new_author][vpaper]=True
                self._multiplex_citation[vpaper][new_author]=True

            #add collaborations, if older, registered collaborations do not exist
            a1_gt_id = self._collab_graphml_vertex_id_to_gt_id[author1]
            a2_gt_id = self._collab_graphml_vertex_id_to_gt_id[author2]
            e = self.collab.edge(a1_gt_id, a2_gt_id)
            
            if e == None:
                e = self.collab.add_edge(a1_gt_id, a2_gt_id)
                self.collab.edge_properties['weight'][e] = 1
                self.collab.edge_properties['dates'][e] = CollabDates( parse_date(year) )
            elif parse_date(year) not in self.collab.edge_properties['dates'][e]:
                self.collab.edge_properties['weight'][e] += 1
                self.collab.edge_properties['dates'][e].add_date( parse_date(year) )
                
            if    None in (self.collab.edge_properties['first_year_collaborated'][e], parse_date(year))   or   self.collab.edge_properties['first_year_collaborated'][e] > parse_date(year):
                self.collab.edge_properties['first_year_collaborated'][e]=parse_date(year)


###############################################################
# MS - Function to read collab from db
    def read_db_create_collab(self, conn, sql, paper_column=0,author_column=1):
        '''Reads meta data from DB, adds these infos to the citation network and builds the collaboration network.'''
        print 'Make sure that the SQL query returns doi, author_id and date'
        conn = psycopg2.connect( **conn )
        cur = conn.cursor()
        cur.execute( sql )
        cou=0
        t_prev=time.time()
        t_cum=0

        for line in cur:

            cou+=1
            if cou-10000*(cou/10000)==0:
                print 'Lines read: '+str(cou)
                t=time.time()   
                t_cum+=t-t_prev
                t_prev=t
                print 'Time passed: '+str(t_cum)

            tmp=list(line)
            author_id=tmp[author_column]
            paper_id=tmp[paper_column]
            year=tmp[2]#.timetuple()[0]   # date is imported as datetime object

            try:
                #see whether paper is already in
                paper = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id])
                self.citation.vertex_properties['year'][paper]=year
                # add the citation dates to the citation network
                for citation in paper.in_edges():
                    self.citation.edge_properties['year'][citation]=year
            except KeyError:
                #otherwise add it
                self.add_paper(paper_id,year,[author_id],update_collaborations=False)
                paper = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id])


## TODO add collaboration weights with timestamps
            coauth = self._multiplex_citation[paper].keys()
            for i in coauth:
                coauthor_id=self.collab.vertex_properties['_graphml_vertex_id'][i]
                self.add_collaboration(author_id,coauthor_id,year)
            self.add_multiplex(paper_id,author_id,year)

        cur.close()
        conn.close()



###############################################################
    # MS - Function to read meta data into a custom property map        
    def read_prop(self, conn, sql, name, tp = 'object', p_or_a = 'p', v_or_e = 'v'):
        '''Reads meta data from DB and adds as a property map'''
        print '''Make sure that the SQL query returns doi and 
        property in case of (v)ertex property and doi, doi, property in case of (e)dge property'''

        if v_or_e[0]=='v' and p_or_a[0]=='p':
            self.citation.vertex_properties[name]=self.citation.new_vertex_property(tp)
        elif v_or_e[0]=='v' and p_or_a[0]=='a':
            self.collab.vertex_properties[name]=self.collab.new_vertex_property(tp)
        elif v_or_e[0]=='e' and p_or_a[0]=='p':
            self.citation.edge_properties[name]=self.citation.new_edge_property(tp)
        elif v_or_e[0]=='e' and p_or_a[0]=='a':
            self.collab.edge_properties[name]=self.collab.new_edge_property(tp)

        conn = psycopg2.connect( **conn )
        cur = conn.cursor()
        cur.execute( sql )
        cou=0
        t_prev=time.time()
        t_cum=0

        for line in cur:

            cou+=1
            if cou-10000*(cou/10000)==0:
                print 'Lines read: '+str(cou)
                t=time.time()
                t_cum+=t-t_prev
                t_prev=t
                print 'Time passed: '+str(t_cum)

            tmp=list(line)
            elem = tmp[0]
            prop_value=tmp[1]

            if v_or_e[0]=='v' and p_or_a[0]=='p':
                try:
                    paper = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[ elem ]) #see whether the elem is already in
                    self.citation.vertex_properties[name][paper] = prop_value
                except KeyError:
                    pass #otherwise pass

            elif v_or_e[0]=='v' and p_or_a[0]=='a':
                try:
                    author = self.collab.vertex(self._collab_graphml_vertex_id_to_gt_id[ elem ]) #see whether the elem is already in
                    self.collab.vertex_properties[name][author] = prop_value
                except KeyError:
                    pass #otherwise pass

            if v_or_e[0]=='e' and p_or_a[0]=='p':   # yet to be written
                pass 
            if v_or_e[0]=='e' and p_or_a[0]=='a':   # yet to be written
                pass

        cur.close()
        conn.close()
################################################################        
    ##
    #Function to read collab from meat-file
    def read_meta_create_collab(self,meta_file, header=True,paper_column=0,author_column=1,delimiter=' '):
        '''Reads meta data file, adds these infos to the citation network and builds the collaboration network.'''
        with open(meta_file,'r') as f:
            
            if header==True:
                f.readline()
            
            cou=0
            t_prev=time.time()
            t_cum=0
            
            for line in f:
                
                cou+=1
                if cou-10000*(cou/10000)==0:
                    print 'Lines read: '+str(cou)
                    t=time.time()
                    t_cum+=t-t_prev
                    t_prev=t
                    print 'Time passed: '+str(t_cum)
                
                tmp=line.split(delimiter)
                author_id=tmp[author_column]
                paper_id=tmp[paper_column]
                year=parse_date(tmp[2].rstrip())
                
                try:
                    paper = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id]) #see whether paper is already in
                    self.citation.vertex_properties['year'][paper]=year
                except KeyError:
                    self.add_paper(paper_id,year,[author_id],update_collaborations=False) #otherwise add it
                    paper = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id])
                
                
                
                coauth = self._multiplex_citation[paper].keys()
                for i in coauth:
                    coauthor_id=self.collab.vertex_properties['_graphml_vertex_id'][i]
                    self.add_collaboration(author_id,coauthor_id,year)
                self.add_multiplex(paper_id,author_id,year)

################################################################        
    ##
    #Function to read citation graphml file
    def read_citation_graphml(self,citation_file):
        '''Reads a citation graphml file and writes the citation layer.'''
        self.citation = gt.load_graph(citation_file)
        
        self.citation.vertex_properties['year']=self.citation.new_vertex_property('object')
        
        for v in self.citation.vertices():
            self._multiplex_citation[v]={}

        #since I do not know how to address a node in graph_tool using his properties, create a dictionary to have this info:
        self._citation_graphml_vertex_id_to_gt_id = {}

        for v in self.citation.vertices(): 
            self._citation_graphml_vertex_id_to_gt_id[self.citation.vertex_properties['_graphml_vertex_id'][v]]=int(self.citation.vertex_index[v])
        

################################################################        
    ##
    #Function to read a multiplex from files
    def read_graphml(self,collab_file,citation_file,mult_file):
        '''Read multiplex from files specifying the collaboration network, the citation network and multiplex meta data'''

        ##################################
        #determine csv delimiter
        f=open(mult_file,'r')
        dialect=csv.Sniffer().sniff(f.readline())
        csv_delimiter=dialect.delimiter
        f.close()

        #read data
        self.collab = gt.load_graph(collab_file)
        self.citation = gt.load_graph(citation_file)
        self.citation.vertex_properties['year']=self.citation.new_vertex_property('object')

        #create the multiplex structure, implemented with property maps
        self._multiplex_collab = self.collab.new_vertex_property('object')
        self._multiplex_citation = self.citation.new_vertex_property('object')

        for v in self.collab.vertices():
            self._multiplex_collab[v]={}
        for v in self.citation.vertices():
            self._multiplex_citation[v]={}

        #since I do not know how to address a node in graph_tool using his properties, create a dictionary to have this info:
        self._collab_graphml_vertex_id_to_gt_id = {}
        self._citation_graphml_vertex_id_to_gt_id = {}

        for v in self.collab.vertices(): 
            self._collab_graphml_vertex_id_to_gt_id[self.collab.vertex_properties['_graphml_vertex_id'][v]]=int(self.collab.vertex_index[v])

        for v in self.citation.vertices(): 
            self._citation_graphml_vertex_id_to_gt_id[self.citation.vertex_properties['_graphml_vertex_id'][v]]=int(self.citation.vertex_index[v])

        #fill the multiplex
        with open(mult_file,'r') as f:
            #read header to determine property name
            header = f.readline()
            header = header.split(csv_delimiter)
            multiplex_edge_property_name = header[2].rstrip()

            #write multiplex edges with multiplex edge property (year)
            for line in f:
                tmp = line.split(csv_delimiter)
                paper_tmp = tmp[0]
                author_tmp = tmp[1]
                year = parse_date(tmp[2].rstrip())

                try:
                    paper_obj = self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_tmp])
                except KeyError:
                    v=self.citation.add_vertex()
                    self.citation.vertex_properties['_graphml_vertex_id'][v]=paper_tmp
                    self._multiplex_citation[v]={}
                    paper_obj = self.add_paper(paper_tmp,year,author_tmp,update_collaborations=False)

                try:
                    author_obj = self.collab.vertex(self._collab_graphml_vertex_id_to_gt_id[author_tmp])
                except KeyError:
                    v=self.collab.add_vertex()
                    self.collab.vertex_properties['_graphml_vertex_id'][v]=author_tmp
                    self._multiplex_collab[v]={}
                    author_obj = v
                    
                self.citation.vertex_properties['year'][paper_obj]=year

                self._multiplex_collab[author_obj][paper_obj] = True
                self._multiplex_citation[paper_obj][author_obj] = True

################################################################
    ##
    #Show all papers by one author
    def papers_by(self,author_id):
        '''Returns a list of paper (citation) vertex objects that specified author has (co)authored.'''
        try:
            author=self.collab.vertex(self._collab_graphml_vertex_id_to_gt_id[author_id])
            return self._multiplex_collab[author].keys()
        except KeyError:
            raise NoSuchAuthorError()
        
################################################################
    ##
    #Show all papers by one author
    def authors_of(self,paper_id):
        '''Returns a list of author (collaboration) vertex objects that have (co)authored the specified paper.'''
        try:
            paper=self.citation.vertex(self._citation_graphml_vertex_id_to_gt_id[paper_id])
            return self._multiplex_citation[paper].keys()
        except KeyError:
            raise NoSuchPaperError()
            
################################################################
    ##
    #Degree sequence of citation->collaboration links = "distribution" of # authors/paper
    def distribution_authors(self,paper_vertex_iterator):
        '''Returns a list of the number of authors for the papers specified in the iterator'''
        number_authors=[]
        for v in paper_vertex_iterator:
            number_authors.append(len(self._multiplex_citation[v].keys()))
        return number_authors
        

################################################################
    ##
    #Degree sequence of collaboration->citation links = "distribution" of # papers/author
    def distribution_papers(self,author_vertex_iterator):
        '''Returns a list of the number of papers for the authors specified in the iterator'''
        number_papers=[]
        for v in author_vertex_iterator:
            number_papers.append(len(self._multiplex_collab[v].keys()))
        return number_papers
    
################################################################
    ##
    #Function to multiplex-map proeprty maps, eventually aggregating and aggregation function
    def multiplex_property_mapping(self,origin_layer_iterator,origin_layer_property,target_layer_property,direction=None,aggregation_function=None):
        '''Returns list of collaboration net properties for selection of nodes and their according multiplex-mapped property, aggregated using aggregation_function.'''
    
        if direction == None:
            print "###################################"
            print "Specify direction of mapping first!"
            print "USE direction='collab_to_citation' OR direction='citation_to_collab'"
            print "####################################"
            return
    
        if direction == 'collab_to_citation':
    
            origin_layer_property_values=[]
            target_layer_property_values=[]
    
            if aggregation_function==None:
            
                print "##############################"
                print "Assuming one-to-one multiplex!"
                print "Consider checking this assumption using check_one_to_one()!"
                print "Otherwise, specify aggregation function!"
                print "##############################"
            
                for v in origin_layer_iterator:
                    try:
                        target_vertex = self._multiplex_collab[v].keys()[0]
                        origin_layer_property_values.append(origin_layer_property[v])
                        target_layer_property_values.append(target_layer_property[target_vertex])
                    except IndexError: #if there is no target vertex, simply don't consider it
                        pass
            else:
                for v in origin_layer_iterator:
                    try:
                        target_vertex = self._multiplex_collab[v].keys()[0]
                        origin_layer_property_values.append(origin_layer_property[v])
                        target_layer_property_values_TMP=[]
                        for target_vs in self._multiplex_collab[v].keys():
                            target_layer_property_values_TMP.append(target_layer_property[target_vs])
                        target_layer_property_values.append(aggregation_function(target_layer_property_values_TMP))
                    except IndexError: #if there is no target vertex, simply don't consider it
                        pass
                    
            return origin_layer_property_values, target_layer_property_values
                    
                    
        if direction == 'citation_to_collab':
            origin_layer_property_values=[]
            target_layer_property_values=[]
    
            if aggregation_function==None:
                print "Assuming one-to-one multiplex!"
                print "Consider checking this assumption using check_one_to_one()!"
                print "Otherwise, specify aggregation function!"
                for v in origin_layer_iterator:
                    try:
                        target_vertex = self._multiplex_citation[v].keys()[0]
                        origin_layer_property_values.append(origin_layer_property[v])
                        target_layer_property_values.append(target_layer_property[target_vertex])
                    except IndexError: #if there is no target vertex, simply don't consider it
                        pass
            else:
                for v in origin_layer_iterator:
                    try:
                        self._multiplex_citation[v].keys()[0]
                        origin_layer_property_values.append(origin_layer_property[v])
                        target_layer_property_values_TMP=[]
                        for target_vs in self._multiplex_citation[v].keys():
                            target_layer_property_values_TMP.append(target_layer_property[target_vs])
                        target_layer_property_values.append(aggregation_function(target_layer_property_values_TMP))
                    except IndexError: #if there is no target vertex, simply don't consider it
                        continue
                
            return origin_layer_property_values, target_layer_property_values



################################################################
    ##
    #Function to calculate shortest path in collab network at time of publication
    def shortest_path_collab_formation(self,new_collab_year):
        '''Calculate shortest path at time of first collaboration'''
    
        shortest_distances={}
    
        mask_collab = self.collab.new_edge_property('bool')

        for year in [new_collab_year]:
            print year
            new_collabs=gt.graph_tool.util.find_edge(self.collab,self.collab.edge_properties['first_year_collaborated'],year)
            #set filter of collabs younger than year
            mask_collab.a = False
            for e in gt.graph_tool.util.find_edge_range(self.collab,self.collab.edge_properties['first_year_collaborated'],[1892,year-1]):
                mask_collab[e] = True
        
            #Set filters for analysis
            self.collab.set_edge_filter(mask_collab)
        
            #calculate shortest distance for all first-time-collabs of year
            for e in new_collabs:
                source = e.source()
                target = e.target()
                shortest_distances[e] = gt.graph_tool.topology.shortest_distance(self.collab,source,target)
            
            #reset graph filters
            self.collab.set_edge_filter(None)
            self.collab.set_vertex_filter(None)
    
        return shortest_distances


################################################################
    ##
    #Function to calculate multiplex neighbourhood of v in layer 1. 
    #Defined as nodes in layer 1, that are the multiplex maps of neighbours of the multiplex map of v.
    def multiplex_neighbours(self,vertex_object,layer=None):
        'Returns an iterator of vertices in layer, that are multiplex neighbours of vertex_object.'
        
        
        #define helper functions, necessary as using a lambda function would disabkle pickling of objects later ...
        def ret_multiplex_citation_key(x):
            return self._multiplex_citation[x].keys()
        
        def ret_multiplex_collab_key(x):
            return self._multiplex_collab[x].keys()
        
        
        if layer==None:
            print "###################################"
            print "Specify start_layer of mapping first!"
            print "USE layer='collab' OR layer='citation'"
            print "####################################"
            return
                
        if layer=='collab':
            multiplex_neighbours_TMP=itertools.imap(ret_multiplex_citation_key,self._multiplex_collab[vertex_object].keys())
            multiplex_neighbours=itertools.chain.from_iterable(multiplex_neighbours_TMP)
            return multiplex_neighbours
        
        if layer=='citation':
            multiplex_neighbours_TMP=itertools.imap(ret_multiplex_collab_key,self._multiplex_citation[vertex_object].keys())
            multiplex_neighbours=itertools.chain.from_iterable(multiplex_neighbours_TMP)
            return multiplex_neighbours


################################################################
    ##
    #Function to get vertex_id's from vertex objects
    def vertex_id(self,iterable_of_vertices,layer=None):
        'Returns an iterator of vertex id strings of the vertex objects specified in iterable_of_vertices, being members of layer.'
    
        #define helper functions, necessary as using a lambda function would disabkle pickling of objects later ...
        def ret_collab_vertex_prop(x):
            return self.collab.vertex_properties['_graphml_vertex_id'][x]
            
        def ret_citation_vertex_prop(x):
            return self.citation.vertex_properties['_graphml_vertex_id'][x]
        
        
        if layer==None:
            print "###################################"
            print "Specify layer of vertex origin!"
            print "USE layer='collab' OR layer='citation'"
            print "####################################"
            return
        
        if layer=='collab':
            return itertools.imap(ret_collab_vertex_prop,iterable_of_vertices)
    
        if layer=='citation':
            return itertools.imap(ret_citation_vertex_prop,iterable_of_vertices)

################################################################
    ##
    #Function to calculate socially biased citations
    def socially_biased_citations(self):
        '''Calculate number of socially-biased citations'''
        print 'Calculating socially biased citation statistics...'
        print '--------------'
        print 'Consider executing check_citation_causality() first!'
        citation_dictionary={}
        for paper in self.citation.vertices():
            year = self.citation.vertex_properties['year'][paper]
            biased_citations=0
            self_citations=0
            citations=0
            authors = self._multiplex_citation[paper].keys()
            earlier_collaborators = []

            for a in authors:
                for n in a.all_neighbours():
                    if self.collab.edge_properties['first_year_collaborated'][self.collab.edge(a,n)]< year:
                        earlier_collaborators.append(n)
                
            for citing_paper in paper.out_neighbours():
                citations+=1
                citing_authors = self._multiplex_citation[citing_paper].keys()
                if set(authors).intersection(set(citing_authors)): #count self-citations
                    self_citations+=1
                    continue #if continue is not given, the three citation counts are not additive, i.e. a self-citation can additionally be a  socially biased citation
                if earlier_collaborators and set(earlier_collaborators).intersection(set(citing_authors)).difference(authors): #add biased citation if citing author is former coauthor of at least one of the authors; exclude self-citations here
                    biased_citations+=1
            citation_dictionary[self.citation.vertex_properties['_graphml_vertex_id'][paper]]=[citations,self_citations,biased_citations]

            # print '--------------'
            # print 'paper: '+self.citation.vertex_properties['_graphml_vertex_id'][paper]
            # print 'citations: '+str(citations)
            # print 'self citation: '+str(self_citations)
            # print 'socially biased citations: '+str(biased_citations)
        print 'Output Format: {paper:[citations,self citations, socially biased citations],... }'
        return citation_dictionary


 
################################################################
    ##
    #Function to calculate citations of papers in years yr after yd years
    def citation_success(self,yr,yd,perc):
        #create property map
        citation_success=self.citation.new_vertex_property("double")
        citation_success_perc=self.citation.new_vertex_property("bool")    
        perc_cuts=[]
            
        for y in yr:
            print y,'...'
            #find vertices
            y1_vertices = gt.find_vertex(self.citation,self.citation.vertex_properties['year'],y)
            y1yd_vertices = gt.find_vertex_range(self.citation,self.citation.vertex_properties['year'],[y,y+yd])
    
            #set vertex filter property
            print 'Set filter prop...'
            y1yd_filter_prop=self.citation.new_vertex_property("bool")
            y1_filter_prop=self.citation.new_vertex_property("bool")
            y1yd_filter_prop.a=False
            y1_filter_prop.a=False
            for v in y1yd_vertices:
                y1yd_filter_prop[v]=True
            for v in y1_vertices:
                y1_filter_prop[v]=True
    
            #calculate graph_view of the subgraph of y,y+yd
            print 'Calc graph view ...'
            sub_cite_degree = self.citation.new_vertex_property("double")
            self.citation.set_vertex_filter(y1yd_filter_prop)
            sub_cite_degree.fa = self.citation.degree_property_map('out').fa
            #there are a lot of zeros ... so the percentile percentage has to be quite high
            self.citation.set_vertex_filter(None)
            self.citation.set_vertex_filter(y1_filter_prop)
            tmp = sub_cite_degree.fa
            percentile_cut = numpy.percentile(tmp,perc)
            perc_cuts.append(percentile_cut)
            print 'Percentile cut is ',percentile_cut
            self.citation.set_vertex_filter(None)

        
            print 'Write success ...'

            #write number of citations and success bool after yd years
            self.citation.set_vertex_filter(y1_filter_prop)
            citation_success.fa = tmp.copy()
            print 'There are ',numpy.count_nonzero(tmp>percentile_cut),' nodes exceeding the percentile cut.'
            citation_success_perc.fa = (tmp > percentile_cut).copy()
            self.citation.set_vertex_filter(None)
        
            
        return citation_success, citation_success_perc,perc_cuts
        
        

        
################################################################
    ##
    #Pickle the multiplex structure
    def save(self,filename):
        #f = open(filename+'_citation.pickle', 'wb')
        self.citation.save(filename+'_citation.gt')
        #pickle.dump(self.citation, f)
        #f.close()

        self.collab.save(filename+'_collaboration.gt')
        #f = open(filename+'_collaboration.pickle','wb')
        #pickle.dump(self.collab,f)
        #f.close()

        f = open(filename+'_citation_ids.pickle','wb')
        pickle.dump(self._citation_graphml_vertex_id_to_gt_id,f)
        f.close()

        f = open(filename+'_collab_ids.pickle','wb')
        pickle.dump(self._collab_graphml_vertex_id_to_gt_id,f)
        f.close()
    
        f = open(filename+'_citation_multiplex.pickle','wb')
        tmp={}
        for v in self.citation.vertices():
            v_id=self.citation.vertex_index[v]
            tmp[v_id]={}
            for w in self._multiplex_citation[v].keys():
                if self._multiplex_citation[v][w]==True:
                    tmp[v_id][self.collab.vertex_index[w]]=True
        pickle.dump(tmp,f)
        f.close()


        f = open(filename+'_collab_multiplex.pickle','w')
        tmp={}
        for v in self.collab.vertices():
            v_id=self.collab.vertex_index[v]
            tmp[v_id]={}
            for w in self._multiplex_collab[v].keys():
                if self._multiplex_collab[v][w]==True:
                    tmp[v_id][self.citation.vertex_index[w]]=True
        pickle.dump(tmp,f)
        f.close()

        with zipfile.ZipFile(filename+'_pkl.zip', 'w', compression=zipfile.ZIP_DEFLATED) as saved:
            saved.write(filename+'_citation.gt', os.path.basename(filename)+'_citation.gt')
            os.remove(filename+'_citation.gt')
            saved.write(filename+'_collaboration.gt', os.path.basename(filename)+'_collaboration.gt')
            os.remove(filename+'_collaboration.gt')
            saved.write(filename+'_citation_ids.pickle', os.path.basename(filename)+'_citation_ids.pickle')
            os.remove(filename+'_citation_ids.pickle')
            saved.write(filename+'_collab_ids.pickle', os.path.basename(filename)+'_collab_ids.pickle')
            os.remove(filename+'_collab_ids.pickle')
            saved.write(filename+'_citation_multiplex.pickle', os.path.basename(filename)+'_citation_multiplex.pickle')
            os.remove(filename+'_citation_multiplex.pickle')
            saved.write(filename+'_collab_multiplex.pickle', os.path.basename(filename)+'_collab_multiplex.pickle')
            os.remove(filename+'_collab_multiplex.pickle')
            

################################################################
    ##
    #Unpickle Multiplex Structure
    def load(self,filename):
        f = os.path.basename(filename)
        if filename[-8:] != '_pkl.zip':
            filename += '_pkl.zip'
            
        with zipfile.ZipFile(filename, 'r') as saved:
            self.citation = gt.load_graph(saved.open(f+'_citation.gt'))
            self.collab = gt.load_graph(saved.open(f+'_collaboration.gt'))
            self._citation_graphml_vertex_id_to_gt_id = pickle.load(saved.open(f+'_citation_ids.pickle'))
            self._collab_graphml_vertex_id_to_gt_id = pickle.load(saved.open(f+'_collab_ids.pickle'))
            tmp = pickle.load(saved.open(f+'_citation_multiplex.pickle'))
            for v_id in tmp.keys():
                v=self.citation.vertex(v_id)
                self._multiplex_citation[v]={}
                for w_id in tmp[v_id].keys():
                    if tmp[v_id][w_id]==True:
                        w=self.collab.vertex(w_id)
                        self._multiplex_citation[v][w]=True
            tmp = pickle.load(saved.open(f+'_collab_multiplex.pickle'))
            for v_id in tmp.keys():
                v=self.collab.vertex(v_id)
                self._multiplex_collab[v]={}
                for w_id in tmp[v_id].keys():
                    if tmp[v_id][w_id]==True:
                        w=self.citation.vertex(w_id)
                        self._multiplex_collab[v][w]=True
        #self.citation = gt.load_graph(filename+'_citation.gt')
        #f =  open(filename+'_citation.pickle','r')
        #self.citation=pickle.load(f)
        #f.close()
        
        #self.collab = gt.load_graph(filename+'_collaboration.gt')        
        #f = open(filename+'_collaboration.pickle','r')
        #self.collab=pickle.load(f)
        #f.close()

        #f = open(filename+'_citation_ids.pickle','r')
        #self._citation_graphml_vertex_id_to_gt_id=pickle.load(f)
        #f.close()

        #f = open(filename+'_collab_ids.pickle','r')
        #self._collab_graphml_vertex_id_to_gt_id=pickle.load(f)
        #f.close()
    
        #f = open(filename+'_citation_multiplex.pickle','r')
        #tmp=pickle.load(f)
        #for v_id in tmp.keys():
        #    v=self.citation.vertex(v_id)
        #    self._multiplex_citation[v]={}
        #    for w_id in tmp[v_id].keys():
        #        if tmp[v_id][w_id]==True:
        #            w=self.collab.vertex(w_id)
        #            self._multiplex_citation[v][w]=True
        #f.close()


        #f = open(filename+'_collab_multiplex.pickle','r')
        #tmp=pickle.load(f)
        #for v_id in tmp.keys():
        #    v=self.collab.vertex(v_id)
        #    self._multiplex_collab[v]={}
        #    for w_id in tmp[v_id].keys():
        #        if tmp[v_id][w_id]==True:
        #            w=self.citation.vertex(w_id)
        #            self._multiplex_collab[v][w]=True
        #f.close()
    
        
                        
##################################################################################################################
##################################################################################################################
#Define other classes and module-wide functions

class CollabDates():
    
    def __init__(self, year=None):
        self.dates = numpy.array([])
        if year:
            self.dates = numpy.append( self.dates,  parse_date(year) )

    def __eq__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates == y

    def __ne__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates  != y

    def __gt__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates > y

    def __lt__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates  < y

    def __ge__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates >= y

    def __le__(self, year):
        if isinstance(year, CollabDates):
            year = year.dates[0]
        print year
        y = parse_date(year)
        return self.dates  <= y

    
    def __iter__(self):
        for d in self.dates:
                yield d
                
    def add_date(self, year):
        self.dates = numpy.append( self.dates,  parse_date(year) )


        
########## LOAD A MULTILAYER NETWORK
def load(filename):
    '''
    Create a `multiplex` object and populate it from a ZIP pickle
    '''

    M = PaperAuthorMultiplex()
    M.load(filename)
    return M
    

################################################################
#Function to check whether multiplex is one-to-one
def check_one_to_one(multiplex):
    'Check whether the multiplex is a one-to-one multiplex'
    
    print '#####################'
    
    multiplex_citation_is_OneToOne=True
    for v in multiplex.citation.vertices():
        if len(multiplex._multiplex_citation[v].keys())>1:
            multiplex_citation_is_OneToOne=False
            print 'citation->collaboration is NOT one-to-one!'
            break
    if multiplex_citation_is_OneToOne==True:
        print 'citation->collaboration is one-to-one.'
            
    multiplex_collab_is_OneToOne=True
    for v in multiplex.collab.vertices():
        if len(multiplex._multiplex_collab[v].keys())>1:
            multiplex_collab_is_OneToOne=False
            print 'collaboration->citation is NOT one-to-one!'
            break
    if multiplex_collab_is_OneToOne==True:
        print 'collaboration->citation is one-to-one.'
    print '#####################'


#################################################
#date parser
def parse_date(timestmp):
    if timestmp is None:
        return None
    try:
        # if the year is not provided, this will assign '1', which will be detected in data analysis
        d = parser.parse(str(timestmp), default=datetime.datetime(1, 1, 1, 0, 0))
        return d.date()
    except:  
        print "Error when reading date:", sys.exc_info()[0]
        print timestmp
        raise





#################################################
#define Error Classes

class PaperIDExistsAlreadyError(Exception):
    pass
    
class NoSuchPaperError(Exception):
    pass
    
class NoSuchAuthorError(Exception):
    pass
    
class CitationExistsAlreadyError(Exception):
    pass

class NotOneToOneError(Exception):
    pass
