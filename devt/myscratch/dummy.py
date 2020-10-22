import xml.etree.cElementTree as ET
tree = ET.parse('/data2/dockets/sqlite/code/devt/myscratch/party-block-example.xml')
for elem in tree.getiterator():
    if elem.tag:
        print 'my name:'
        print '\t'+elem.tag
    if elem.text:
        print 'my text:'
        print '\t'+(elem.text).strip()
    if elem.attrib.items():
        print 'my attributes:'
        for key, value in elem.attrib.items():
            print '\t'+'\t'+key +' : '+value
    if list(elem): # use elem.getchildren() for python2.6 or before
        print 'my no of child: %d'%len(list(elem))
    else:
        print 'No child'
    if elem.tail:
        print 'my tail:'
        print '\t'+'%s'%elem.tail.strip()
    print '$$$$$$$$$$'
