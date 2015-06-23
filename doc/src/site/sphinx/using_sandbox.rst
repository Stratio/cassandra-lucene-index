Sandbox and demo
****************

Vagrant Setup
=============

To get an operating virtual machine with Stratio Deep distribution up
and running, we use `Vagrant <https://www.vagrantup.com/>`__.

-  Download and install
   `Vagrant <https://www.vagrantup.com/downloads.html>`__.
-  Download and install
   `VirtualBox <https://www.virtualbox.org/wiki/Downloads>`__.
-  If you are in a windows machine, we will install
   `Cygwin <https://cygwin.com/install.html>`__.

Running the sandbox
===================

-  Initialize the current directory from the command line:
   **``vagrant init stratio/deep-spark``**.
-  Start the sandbox from command line: **``vagrant up``**

Please, be patient the first time it runs!!

Login into the sandbox as root user and start the services: - Start
Spark and Stratio Deep: **``service spark start``** - Start Cassandra:
**``service cassandra start``**

What you will find in the sandbox
=================================

-  OS: CentOS 6.5
-  6GB RAM - 2 CPU
-  Two ethernet interfaces.

Name \| Version \| Service name \| Other

Stratio Cassandra \| 2.1.05 \| cassandra \| service cassandra start

Stratio Deep \| 0.6 \| \| service streaming start

Spark \| 1.1.0 \| spark \| service spark start

Mongodb \| 2.6.5 \| mongod \| service mongod start

Access to the sandbox and other useful commands
===============================================

Useful commands
---------------

-  Start the sandbox: **``vagrant up``**
-  Shut down the sandbox: **``vagrant halt``**
-  In the sandbox, to exit to the host: **``exit``**

Accessing the sandbox
---------------------

-  Located in /install-folder
-  **``vagrant ssh``**

Starting the Stratio Cassandra CQL Shell
========================================

From the sandbox (vagrant ssh):

-  Starting the Stratio Cassandra CQL Shell: **``/bin/cqlsh``**
-  Exit the Stratio Stratio Cassandra CQL Shell: **``exit;``**

F.A.Q about the sandbox
=======================

I am in the same directory that I copy the Vagrant file but I have this error:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

        A Vagrant environment or target machine is required to run this
        command. Run vagrant init to create a new Vagrant environment. Or,
        get an ID of a target machine from vagrant global-status to run
        this command on. A final option is to change to a directory with a
        Vagrantfile and to try again.

Make sure your file name is Vagrantfile instead of Vagrantfile.txt or
VagrantFile.


When I execute vagrant ssh I have this error:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

        ssh executable not found in any directories in the %PATH% variable. Is an
        SSH client installed? Try installing Cygwin, MinGW or Git, all of which
        contain an SSH client. Or use your favorite SSH client with the following
        authentication information shown below:

We need to install `Cygwin <https://cygwin.com/install.html>`__ or `Git
for Windows <http://git-scm.com/download/win>`__.
