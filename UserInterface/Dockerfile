FROM node:18 AS FRONTEND
COPY ./ocid_project/ ./ocid_project/
RUN cd ocid_project && npm install && npm run build

FROM node:16 as BACKEND
COPY ./backend/ ./backend/
COPY --from=FRONTEND ./ocid_project/dist/ocid_project/ ./backend/dist/
RUN cd backend && npm install
EXPOSE 3000

CMD [ "node", "./backend/backend.js" ]