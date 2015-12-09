from __future__ import division
# from knimin import TRN


class Protocol(object):
    @classmethod
    def iter_all(cls):
        """Returns list of all protocols in the system

        Returns
        -------
        list of Protocol objects
            All protocols available in the system
        """

    @classmethod
    def create(cls, name, description, steps):
        """Creates a new protocol on the system

        Parameters
        ----------
        name : str
            name of the protocol
        description : str
            Human interpretable summary of the protocol
        steps : list of Step objects
            Steps making up the protocol, in order of action

        Returns
        -------
        Protocol object
            The new protcol
        """

    @classmethod
    def delete(cls, protocol_id):
        """Deletes a protocol from the system

        Parameters
        ----------
        protocol_id : int
            Protocol ID to initialize

        Returns
        -------
        bool
            whether deletion was successful or not
        """

    def __init__(self, protocol_id):
        """Initialzies a protocol

        Parameters
        ----------
        protocol_id : int
            Protocol ID to initialize
        """
        self._id = protocol_id
        # TODO: Check if ID is valid in database

    def __str__(self):
        raise NotImplementedError()

    def __len__(self):
        """Number of steps in the protocol"""

    def __eq__(self, other):
        """True if same steps in same order"""

    def __ne__(self, other):
        return not self == other

    def __iter__(self):
        """Iterate over all steps in the protocol"""

    def __contains__(self, step):
        """Check if step exists in protocol

        Parameters
        ----------
        step : Step object

        Returns
        -------
        bool
            Whether step in protocol or not
        """

    def __getitem__(self, position):
        """Get step at position

        Parameters
        ----------
        position : integer

        Returns
        -------
        Step object

        Raises
        ------
        IndexError
            position given does not match with number of steps, 0 indexed
        """

    @property
    def id(self):
        return self._id

    @property
    def steps(self):
        """Steps of the protocol

        Returns
        -------
        tuple of Step objects
            The steps of the protocol, in run order
        """

    def insert_step(self, position, step):
        """Add a new step to the protocol

        Parameters
        ----------
        position : int
            Position in the protocol list of steps to add new step, 0 indexed
        step : Step object
            The step to add
        """

    def remove_step(position):
        """Remove a step from the protocol

        Parameters
        position : int
            Position in the protocol list of steps to remove, 0 indexed
        """


class Step(object):
    @classmethod
    def iter_all(cls):
        """Returns list of all steps in the system

        Returns
        -------
        list of Step objects
            All steps available in the system
        """

    @classmethod
    def create(cls, name, description, reagents=None, instruments=None,
               peripherals=None, primers=None):
        """Creates a new Step on the system

        Parameters
        ----------
        name : str
            name of the step
        description : str
            Human interpretable summary of the step
        reagents : list of Reagent objects, optional
            Reagents used in the step
        instruments : list of Instrument objects, optional
            Instruments used in the step
        peripherals : list of Peripheral objects, optional
            Peripherals used in the step
        primers : list of Primer objects, optional
            Primers used in the step

        Returns
        -------
        step object
            The new step

        Notes
        -----
        At least one of reagents, instruments, peripherals, and/or primers must
        be passed in order to create the step.
        """

    @classmethod
    def delete(cls, step_id):
        """Deletes a Step from the system

        Parameters
        ----------
        step_id : int
            step ID to remove

        Returns
        -------
        bool
            whether deletion was successful or not
        """

    def __init__(self, step_id):
        """Initialzies a step

        Parameters
        ----------
        step_id : int
            step ID to initialize
        """
        self._id = step_id
        # TODO: Check if ID is valid in database

    @property
    def id(self):
        return self._id

    @property
    def reagents(self):
        """Reagents used in this step

        Returns
        -------
        tuple of Reagent objects
        """

    @property
    def instruments(self):
        """Instruments used in this step

        Returns
        -------
        tuple of Instrument objects
        """

    @property
    def peripherals(self):
        """Peripherals used in this step

        Returns
        -------
        tuple of Peripheral objects
        """

    @property
    def primers(self):
        """Primers used in this step

        Returns
        -------
        tuple of Primer objects
        """

    def __str__(self):
        raise NotImplementedError()

    def __eq__(self, other):
        """True if same reagents, instruments, peripherals, and primers"""

    def __ne__(self, other):
        return not self == other
