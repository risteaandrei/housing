FROM amazonlinux
RUN amazon-linux-extras install python3
RUN yum install -y python3-pip zip && yum clean all
# docker run --rm -v ${PWD}:/build lambda_layers pip3.7 install -r /build/requirements.txt -t /build/python/lib/python3.7/site-packages/.
