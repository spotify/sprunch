#!/usr/bin/env python


def mklist(n, fmt, sep=', '):
    l = map(lambda i: fmt.format(i), range(1, n + 1))
    return sep.join(l)


def mk_get_list(n):
    fmt = 'c.get({0}).asInstanceOf[T{1}]'
    l = map(lambda i: fmt.format(i - 1, i), range(1, n + 1))
    return ', '.join(l)

for n in range(5, 23):
    t = mklist(n, 'T{0}')
    p = mklist(n, 'pType{0}: PType[T{0}]')
    g = mk_get_list(n)
    s = mklist(n, 's._{0}.asInstanceOf[AnyRef]')
    a = mklist(n, 'pType{0}')
    print '    implicit def scalaTuple%d[%s](implicit %s): PType[(%s)] =' % (
        n, t, p, t
        )
    print '      Avros.derived('
    print '        classOf[(%s)],' % t
    print '        new Fns.SMap[CTupleN, (%s)](c => (%s)),' % (t, g)
    print '        new Fns.SMap[(%s), CTupleN](s => CTupleN.of(%s)),' % (t, s)
    print '        Avros.tuples(%s))' % a
    print
