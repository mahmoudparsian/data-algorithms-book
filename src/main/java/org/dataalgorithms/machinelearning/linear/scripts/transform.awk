# Data Transformation:
#
# the goal is to convert the following format
#
#     Price,Age,KM,FuelType,HP,MetColor,Automatic,CC,Doors,Weight
#
# into the following:
#
#     Price,Age,KM,FuelType1,FuelType2,HP,MetColor,Automatic,CC,Doors,Weight
#
BEGIN { FS = "," }
{
  if ($4 == "FuelType") {
     $4 = "FuelType1,FuelType2"
  }
  else if ($4 == "CNG") {
     $4 = "1,0"
  }
  else if ($4 == "Diesel") {
     $4 = "0,1"
  }
  else if ($4 == "Petrol") {
     $4 = "0,0"
  }  
  print $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $7 "," $8 "," $9 "," $10 
}
    