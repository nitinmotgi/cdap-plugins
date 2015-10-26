for file in `ls -1 resources/ui/*`
do 
  name=`basename $file`
  rm -f ~/Downloads/cdap-sdk-3.2.1/ui/templates/common/$name
  echo $name
done
