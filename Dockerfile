# Dockerfile
FROM node:18-alpine
WORKDIR /usr/src/app
RUN apk add --no-cache curl jq
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
