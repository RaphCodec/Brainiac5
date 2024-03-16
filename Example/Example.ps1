$path = "PATH TO PYTHON FILE FOLDER"
$script_name = "Example.py"

## Don't change anything below this
if ( -not ( Test-Path -Path $path ) ) {
    throw "Path doesn't exist: '$path'"
}
cd $path
if ( -not ( "env/" | Test-Path ) ) {
    echo "Python env/ doesn't exists, creating it now..."
    python -m venv env
}
env\Scripts\Activate.ps1
echo "Installing any missing python dependencies..."
python -m pip install -r python_dependencies.txt | out-null
python $script_name
If ( $LASTEXITCODE -ne 0 ) { ## If the python script threw an error, exit the entire ps1 script
    deactivate
    throw "Python script failed"
}
deactivate