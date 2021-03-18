# Create some folders and files to save files and infos
echo "FOLDERS CREATION.."
mkdir configs
echo "(1/6) - configs folder created!"
mkdir datasets
echo "(2/6) - datasets folder created!"
mkdir logs
echo "(3/6) - logs folder created!"
mkdir scripts
echo "(4/6) - results folder created!"
mkdir results
echo "(5/6) - results folder created!"
mkdir notebooks
echo "(6/6) - results folder created!"

echo "CONFIGS CREATION.."
touch ./configs/global_parameters.csv
echo "variable;value" >> configs/global_parameters.csv
echo "WORKING_PATH;$PWD" >> configs/global_parameters.csv
echo "(1/1) - configurations file about setup created!"