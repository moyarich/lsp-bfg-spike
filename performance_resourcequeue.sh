###Prepare datavvi resourcequene_sri_multiple
python -u lsp.py -s resourcequene_load >./resourcequene_sri_multiple 2>&1

#reuse data and  run workload
python -u lsp.py -s resourcequene_tpch_multiplelevel -a -c   > ./resourcequene_tpch_multiplelevel 2>&1

#4. 2. 1 ratio concurrent to run tpch 10G 
python -u lsp.py -s resourcequene_tpch_ratio_10g -a -c  > ./resourcequene_tpch_ratio_10g 2>&1


#4. 2. 1 ratio concurrent to run tpch 200G 
python -u lsp.py -s resourcequene_tpch_ratio_200g -a -c > ./resourcequene_tpch_ratio_200g 2>&1

#100 stream run single row insert
python -u lsp.py -s resourcequene_sri_multiple   > ./resourcequene_sri_multiple 2>&1
