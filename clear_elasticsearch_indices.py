"""Simple module for deleting Elasticsearch indices older than x days."""

from __future__ import print_function
import sys
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from datetime import datetime, date, timedelta

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.exceptions import NotFoundError, ConnectionError


class ESClient(object):
    """Delete Elasticsearch indices older than x days."""

    def __init__(self, es_nodes, keep_in_days=180, prefix='filebeat-*'):
        """Initialize instance and instance attributes.

        Args:
            es_nodes:	  (list): Elasticsearch nodes (hostname|ip).
            keep_in_days: (int, optional): Number of days to keep indices. Default 180.
                (WARNING! Script will delete indices outside the keep_in_days value!)
            prefix:	  (str, optional): ES index prefix. Example: logstash-*
                Default is `filebeat-*`.
        """
        if not isinstance(es_nodes, list) and not es_nodes:
            raise SystemExit('List of one or more Elasticsearch nodes needed')

        self.es_nodes = es_nodes
        self.prefix = prefix
        self._cutoff_date = date.today() - timedelta(days=keep_in_days)

    def run(self):
        """Connect to Elasticsearch node(s) and delete indices.

        Returns:
            list_of_deleted_indices:	(list): Deleted indices.
        """
        list_of_deleted_indices = []
        for client_obj, node in self._connect_to_elasticsearch():
            list_of_deleted_indices.append(self._delete_indices(client_obj, node))
        return list_of_deleted_indices

    def _delete_indices(self, client, node):
        """Delete indices older than x days.

        Args:
            client:     (obj): Elasticsearch connection object.
            node:       (str): Elasticsearch node hostname/ip address.

        Returns:
            obj:        (dict): Deleted Elasticsearch indices.
        """
        try:
            deleted_indices = dict()
            for index in client.indices.get(index=self.prefix).keys():
                # Create datetime.date() object using date string in indices
                index_date_str = str(index).split('-')[-1]
                index_date_obj = datetime.strptime(index_date_str.replace(
                    '.', ' '), '%Y %m %d').date()
                if index_date_obj < self._cutoff_date:
                    resp = client.indices.delete(index=str(index))
                    deleted_indices[str(index)] = resp.get("acknowledged")
        except NotFoundError:
            pass
        except ConnectionError as e:
            sys.stderr.write(str(e.error) + '\n')
            raise SystemExit("Failed to connect to '{0}'".format(node))
        return {node: deleted_indices}

    def _connect_to_elasticsearch(self):
        """Connect to Elasticsearch nodes using Elasticsearch API.

        Yield:
            obj:     (obj): Elasticsearch connection object.
        """
        for node in self.es_nodes:
            try:
                client_obj = Elasticsearch(node)
            except TransportError as e:
                sys.stderr.write(str(e.error) + '\n')
                raise SystemExit('TransportError: {0}'.format(node))
            yield client_obj, node

    @classmethod
    def email_results(cls, smtp=None, from_addr=None, to_addr=None,
                      subject=None, content=None):
        """Email deleted indices to inbox.

        Args:
            smtp:       (str): SMTP relay server.
            from_addr:	(str): Email address of sender.
            to_addr:    (str): Email address to send results.
            subject:    (str): Email subject.
            content:    (dict): Deleted Elasticsearch indices.
        """
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Subject'] = subject
        body = '{0}'.format(str(content))
        msg.attach(MIMEText(body, 'plain', 'utf-8'))

        mailserver = smtplib.SMTP(smtp)
        mailserver.sendmail(from_addr, to_addr, msg.as_string())
        mailserver.quit()


def main():
    """Manager class method which executes all instance methods.

    Delete Elasticsearch indices older than x days and emailing the results
    to a specified mailbox.
    """
    # Initialize a client
    client = ESClient(['10.100.228.33', '10.126.222.18'])

    # Run delete operation
    list_of_deleted_indices = client.run()

    # Send results to inbox
    ESClient.email_results(
        smtp='localhost',
        from_addr='ansible@creativevirtual.com',
        to_addr='infra_usa@creativevirtual.com',
        subject='Deleted Elasticsearch indices',
        content=list_of_deleted_indices)


if __name__ == '__main__':
    main()
