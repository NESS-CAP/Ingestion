The goal of this repository is to ingest the e-laws html into a Neo4j graph.

There is a schmea defined in schemas.py which outlines the nodes and relationships of the sections

I want you to build the graph in passes first *ONLY* use the html class name to build the hiarchy 
- note that there should be a node per paragraph tag, table etc. Ignore html tags for styling

Next I want to make all the relationships by reading each nodes information and finding the relation


