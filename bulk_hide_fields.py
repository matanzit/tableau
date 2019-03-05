## This Script is used to hide a list of columns in a tableau workbooks (datasources), changes are applied in bulk to the twb xml.

import xml.etree.ElementTree as ET

input_fname='source.twb'
output_fname='target.twb'
fields_to_hide = ['field1','field2']

def main():
    tree = ET.parse(input_fname)
    root = tree.getroot()

    count=0
    hcount=0

    for ds in root.iter('datasource'):
        if 'caption' in ds.attrib and 'inline' in ds.attrib: # and ds.attrib["caption"]=='data_source_1':
            # print ds.attrib
            for col in ds.iter('column'):
                if 'role' in col.attrib and (col.attrib["role"]=='dimension' or col.attrib["role"]=='measure'):
                    count += 1
                    if 'caption' in col.attrib: # if it's a new column that been named
                        print col.attrib["caption"]             # print it
                    else:
                        ET.Element.set(col, 'hidden', 'true')   # else hide it
                        continue
                    if 'hidden' not in col.attrib and col.attrib["caption"] in fields_to_hide:  # if it's named, exposed, but not require - hide it
                        hcount+=1
                        ET.Element.set(col,'hidden','true')
                        # print(ET.tostring(col))   # write the XML full note
                        # print col.attrib          # write the XML node attributes
                        # hide = ET.SubElement(col, 'hidden')   # adding a sub element (not what we need here)
                        # hide.text='true'                      # setting sub element text

    print hcount,'/',count, ' Columns Hidden!'
    tree.write(open(output_fname, 'wb'))

if __name__ == "__main__":
    main();
