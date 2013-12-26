import nose.tools as n
from socrata_homepages import dedupe

def test_dedupe():
    datasets1 = [
        {'id': 'a', 'catalog': 'portal1'},
        {'id': 'b', 'catalog': 'portal1'},
        {'id': 'c', 'catalog': 'portal1'}]
    datasets2 = [
        {'id': 'g', 'catalog': 'portal2'},
        {'id': 'h', 'catalog': 'portal2'},
        {'id': 'c', 'catalog': 'portal2'}]
    edges = [('portal1', 'portal2')]
    observed = dedupe(datasets1 + datasets2, edges)

    # Sort by id
    expected = [
        {'id': 'a', 'catalog': 'portal1'},
        {'id': 'b', 'catalog': 'portal1'},
        {'id': 'c', 'catalog': 'portal2'},
        {'id': 'g', 'catalog': 'portal2'},
        {'id': 'h', 'catalog': 'portal2'},
    ]
    observed_list = list(sorted(observed, key = lambda x: x['id']))
    n.assert_list_equal(observed_list, expected)
