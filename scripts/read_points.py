import sys
import csv

def create_entityType(entity_type_name, input_file):

    # Read points in that look like Tags 1 || Flow Meter
    rows = []
    print("Open File")
    with open(input_file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        point_dimension_values = {
            "label": "",
            "units": "",
            "parameter_name": ""
        }
        metrics = []

        for row in csv_reader:
            if line_count == 0:
                print("Column names are %s" % {", ".join(row)})
                line_count += 1
            else:

                try:
                    parameter_name = row["Point"].replace(' ', '_')
                    # name = row["Point"].replace(' ', '_')
                    print("Name %s" % parameter_name)
                    type = row["DataType"]
                    print("Type %s" % type)
                    if parameter_name == "":
                        break

                    # Pull Name and Type from headers
                    # print("row Point_name %s " %row["Point"] )

                    # Create metric
                    if row["Point_Data_Type"] == "S":
                        print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                        print("________________________ Point db function name  %s " % row["Function"])
                        metric_to_add = {'parameter_name': parameter_name, 'type': type}
                        metrics.append(metric_to_add)

                    # Create Constant
                    if row["Point_Data_Type"] == "C":
                        print("________________________ Point value  %s " % row["Value"])
                        print(
                            "________________________ Point point_data_type  %s " % row["Point_Data_Type"].replace(' ',
                                                                                                                   '_'))
                        print("________________________ Point db function name  %s " % row["Function"])

                    # Create Function
                    if row["Point_Data_Type"] == "F":
                        print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                        print("________________________ Point db data_type  %s " % row["DataType"])
                        print("________________________ Point db function name  %s " % row["Function"])
                except:
                    print(sys.exc_info()[0])  # the exception instance
                    break

    return

def main(args):
    if (len(sys.argv) > 0):
        entity_type_name = sys.argv[1]
        input_file = sys.argv[2]
        print("entity_type_name %s" % entity_type_name)
        print("input_file %s" % input_file)
        create_entityType(entity_type_name=entity_type_name, input_file=input_file)
    else:
        print("Please provide path to csv file as script argument")
        exit()
    print ("Done creating Entity Type ")

if __name__ == "__main__":
   main(sys.argv[1:])
