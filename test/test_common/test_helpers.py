from common.helpers import *

def test_group_by():
    mylist = ["cat","dog","human","elephant","zebra","lions"]
    grouped = group_by(mylist, lambda x : len(x))
    assert grouped == {3: ['cat', 'dog'], 5: ['human', 'zebra', 'lions'], 8: ['elephant']}

if __name__ == '__main__':
    unittest.main()