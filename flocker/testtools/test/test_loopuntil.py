from twisted.internet.defer import CancelledError, Deferred
from twisted.internet.task import deferLater, Clock
from twisted.trial.unittest import SynchronousTestCase, TestCase

from .. import loop_until


class LoopUntilTests(SynchronousTestCase):
    def test_finish(self):
        """
        When the ``predicate`` returns ``True``, the ``Deferred`` returned by
        ``loop_until`` calls back.
        """
        clock = Clock()
        finish = []
        called_back = []

        def predicate():
            return finish == [True]

        looping = loop_until(predicate, reactor=clock)
        looping.addCallback(lambda ignored: called_back.append(True))

        self.assertEqual([], called_back)

        clock.advance(0.1)
        self.assertEqual([], called_back)

        finish.append(True)

        clock.advance(0.1)
        self.assertEqual([True], called_back)

    def test_finish_no_more_predicate(self):
        """
        When the ``predicate`` returns ``True``, the ``predicate`` is not
        called again.
        """
        clock = Clock()
        finish = []
        called = []

        def predicate():
            called.append(True)
            return finish == [True]

        loop_until(predicate, reactor=clock)

        self.assertEqual([True], called)

        clock.advance(0.1)
        self.assertEqual([True]*2, called)

        finish.append(True)

        clock.advance(0.1)
        self.assertEqual([True]*3, called)

        clock.advance(0.1)
        self.assertEqual([True]*3, called)

    def test_cancel_no_more_predicate(self):
        """
        When the ``Deferred`` returned by ``loop_until`` is cancelled the
        predicate is not called again.
        """
        clock = Clock()
        called = []

        def predicate():
            called.append(True)
            return False

        looping = loop_until(predicate, reactor=clock)

        def handle_cancel(failure):
            failure.trap(CancelledError)
        looping.addErrback(handle_cancel)

        self.assertEqual([True], called)

        clock.advance(0.1)
        clock.advance(0.1)

        self.assertEqual([True]*3, called)
        looping.cancel()

        clock.advance(0.1)
        self.assertEqual([True]*3, called)

    def test_cancel_predicate_deferred(self):
        """
        When the ``Deferred`` returned by ``loop_until`` is cancelled the
        predicate
        """
        clock = Clock()
        called = []

        def do_cancel(d):
            called.append(d)

        predicate_deferred = Deferred(do_cancel)

        def predicate():
            return predicate_deferred

        looping = loop_until(predicate, reactor=clock)
        looping.cancel()
        self.assertEqual([predicate_deferred], called)


class LoopUntilFunctionalTests(TestCase):
    def test_finish(self):
        """
        When the ``Deferred`` returned by ``loop_until`` is cancelled the
        predicate is not called again.
        """
        from twisted.internet import reactor

        finish = []
        called = []

        def predicate():
            called.append(True)
            return finish == [True]

        looping = loop_until(predicate)

        def count_calls(ignored):
            self.assertEqual([True] * (20+1), called)

        looping.addCallback(count_calls)

        def stop_loop():
            finish.append(True)
        deferLater(reactor, 2, stop_loop)
        return looping
