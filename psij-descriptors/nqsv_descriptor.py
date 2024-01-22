from distutils.version import StrictVersion

from psij.descriptor import Descriptor

__PSI_J_EXECUTORS__ = [Descriptor(name='nqsv', version=StrictVersion('0.0.1'),
                                   cls='psijnqsv.nqsv.NQSVJobExecutor')]