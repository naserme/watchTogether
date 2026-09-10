FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server ./server
COPY client ./client
ENV PORT=3000 HOST=0.0.0.0
EXPOSE 3000
CMD ["node","server/server.js"]
