
FROM an2071497/logistic_risk_base_img


WORKDIR /extraction

COPY . .

RUN pip install -r requirements.txt

RUN chown -R orca:orca /extraction

USER orca

ENTRYPOINT [ "./entrypoint.sh"]
