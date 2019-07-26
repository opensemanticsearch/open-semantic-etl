import sys
import hashlib
import tempfile
import os
import shutil
import subprocess
from etl_file import Connector_File


class enhance_pst(object):

    def process(self, parameters={}, data={}):

        verbose = False
        if 'verbose' in parameters:
            if parameters['verbose']:
                verbose = True

        filename = parameters['filename']

        # if the processed file was extracted from a PST (parameter container was set), write container setting in data, so the link of the id/content can be set to the PST file
        if 'container' in parameters:
            if not 'container_s' in data:
                data['container_s'] = parameters['container']

        if 'content_type_ss' in data:
            mimetype = data['content_type_ss']
        elif 'content_type_ss' in parameters:
            mimetype = parameters['content_type_ss']
        else:
            mimetype = 'Unknown'

        # if connector returns a list, use only first value (which is the only entry of the list)
        if isinstance(mimetype, list):
            mimetype = mimetype[0]

        # if content type or file ending is PST mailbox start indexing
        if mimetype.lower().startswith('application/vnd.ms-outlook-pst') or filename.lower().endswith('.pst'):
            if verbose:
                print("Mimetype ({}) or file ending seems Outlook PST file, starting extraction of emails".format(
                    mimetype))

            self.pst2email(pstfilename=filename,
                           parameters=parameters, verbose=verbose)

        return parameters, data

    # extract all mails and attachments and index each file with id of container
    def pst2email(self, pstfilename, parameters={}, verbose=False):

        # we build temp dirname ourselfes instead of using system_temp_dirname so we can use configurable / external tempdirs

        if 'tmp' in parameters:
            system_temp_dirname = parameters['tmp']
            if not os.path.exists(system_temp_dirname):
                os.mkdir(system_temp_dirname)
        else:
            system_temp_dirname = tempfile.gettempdir()

        h = hashlib.md5(parameters['id'].encode('UTF-8'))
        temp_dirname = system_temp_dirname + os.path.sep + \
            "opensemanticetl_enhancer_pst_" + \
            str(os.getpid()) + "_" + h.hexdigest()

        if not os.path.exists(temp_dirname):
            os.mkdir(temp_dirname)

        # start external PST extractor / converter
        result = subprocess.call(
            ['readpst', '-S', '-D', '-o', temp_dirname, pstfilename])

        if not result == 0:
            sys.stderr.write(
                "Error: readpst failed for {}".format(pstfilename))

        # prepare document processing
        connector = Connector_File()
        connector.verbose = verbose
        connector.config = parameters.copy()

        # only set container if not yet set by a ZIP or PST before (if this PST is inside another ZIP or PST)
        if not 'container' in connector.config:
            connector.config['container'] = pstfilename

        for dirName, subdirList, fileList in os.walk(temp_dirname):

            if verbose:
                print('Scanning directory: %s' % dirName)

            for fileName in fileList:
                if verbose:
                    print('Scanning file: %s' % fileName)

                try:
                    # replace temp dirname from indexed id
                    contained_dirname = dirName.replace(temp_dirname, '', 1)

                    # build a virtual filename pointing to original PST file

                    if contained_dirname:
                        contained_dirname = contained_dirname + os.path.sep
                    else:
                        contained_dirname = os.path.sep

                    connector.config['id'] = parameters['id'] + \
                        contained_dirname + fileName

                    contained_filename = dirName + os.path.sep + fileName

                    # E-mails filenames are pure number
                    # Attachment file names are number-filename
                    # if temp_filename without - in filename, its a mail file
                    # rename to suffix .eml so Tika will extract more metadata like from and to
                    if not '-' in fileName:
                        os.rename(contained_filename,
                                  contained_filename + '.eml')
                        contained_filename += '.eml'
                        connector.config['id'] += '.eml'

                    try:
                        connector.index_file(filename=contained_filename)

                    except KeyboardInterrupt:
                        raise KeyboardInterrupt

                    except BaseException as e:
                        sys.stderr.write("Exception while indexing contained content {} from {} : {}\n".format(
                            fileName, connector.config['container'], e.message))

                    os.remove(contained_filename)

                except BaseException as e:
                    sys.stderr.write(
                        "Exception while indexing file {} : {}\n".format(fileName, e.message))

        shutil.rmtree(temp_dirname)
