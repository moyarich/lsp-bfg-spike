#### only load data using gpadmin
#python -u lsp.py -s resourcequene_run  > ./resourcequene_run 2>&1

#load data to run workload
python -u lsp.py -s resourcequene_tpch_multiplelevel  > ./resourcequene_tpch_multiplelevel 2>&1

#4. 2. 1 ratio concurrent to run tpch 10G 
python -u lsp.py -s resourcequene_tpch_ratio_200g  > ./resourcequene_tpch_ratio_200g 2>&1


#4. 2. 1 ratio concurrent to run tpch 200G 
python -u lsp.py -s resourcequene_tpch_ratio_200g  > ./resourcequene_tpch_ratio_200g 2>&1

resourcequene_tpch_ratio_200g.yml

#100 stream run single row insert
python -u lsp.py -s resourcequene_sri_multiple   > ./resourcequene_sri_multiple 2>&1
