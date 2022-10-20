from sklearn.preprocessing import StandardScaler, PolynomialFeatures
import numpy as np
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm







class Preprocessor():

    def __init__(
        self,data,target,deg = 2,names = None,interaction_only = True,log_target = True):
        
        self.poly = PolynomialFeatures(
            degree = deg,
            include_bias= False,
            interaction_only=interaction_only
        )
        self.names = np.array(names)
        self.feature_scaler = StandardScaler()
        self.target_scaler = StandardScaler()
        
        if log_target:
            self.idx = target.nonzero()[0]
            self.data = data[self.idx]
            self.target = np.expand_dims(np.log(target[self.idx]),axis = 1)
        else:
            self.data = data
            self.target = np.expand_dims(target,axis = 1)
        self.name_idx = {self.names[i]:i for i in range(len(self.names))}

    def select_variables(self,variables):
        selected = []
        idx = []
        for var in variables:    
            selected.append(
               self.data[:,self.name_idx[var]]
            )
            idx.append(self.name_idx[var])
        return selected,idx 

    def make_polynomial(self)->None:
        self.data = self.poly.fit_transform(self.data)
        self.names = self.poly.get_feature_names_out(self.names)
        self.name_idx = {self.names[i]:i for i in range(len(self.names))}


    def scale_data(self)->None:
        self.data = self.feature_scaler.fit_transform(self.data)
        self.target = self.target_scaler.fit_transform(self.target)

    def convolve_gauss(self,signal:np.array,frac = 0.05,set_data = False) -> np.array:
        window_length = int(frac*signal.size)
        x = np.linspace(-1,1,window_length)
        window = np.exp(-0.5*x**2)/np.sqrt(2*np.pi)
        conv = np.convolve(signal.flatten(),window,"same")
        if set_data:
            self.target = conv.reshape(-1,1)
        return conv

    def convolve_ramp(self,signal:np.array,frac=0.05,set_data = False ) -> np.array:
        window_length = int(frac*signal.size)
        x = np.linspace(-1,1,window_length)
        window = x/window_length
        conv = np.convolve(signal.flatten(),window,"same")
        if set_data:
            self.target = conv.reshape(-1,1)
        return conv
    
    def to_torch(self):
        return (torch.tensor(self.data,dtype=torch.float32),
                torch.tensor(self.target,dtype=torch.float32))
    

class dataset(Dataset):

    def __init__(self, data, target):
        self.N = len(target)
        self.target = target
        self.data = data

    def __len__(self):
        return self.N
    
    def __getitem__(self,index):
        return self.data[index],self.target[index]




class model(nn.Module):

    def __init__(self,in_shape,out_shape):
        super().__init__()
        self.input_feaures = in_shape
        self.output_feaures = out_shape
        self.linear = nn.Linear(in_shape,out_shape,bias = False)
    def forward(self,x):
        return self.linear(x)

    def initialize_weights(self,):
        with torch.no_grad():
            k = 1/torch.sqrt(torch.tensor(self.input_feaures))
            self.linear.weight = nn.Parameter(nn.init.uniform_(self.linear.weight,
            a= -k,
            b= k))


def train(model,optimizer,data,target,cos_sim,n_epochs,lam = 0.5):
    
    for i in range(n_epochs):
        optimizer.zero_grad()
        pred = model(data)
        loss = 1-cos_sim(pred,
        target )
        regloss = lam*torch.norm(model.linear.weight,p = 1)
        L = loss
        L += regloss  
        L.backward()
        optimizer.step()
    
    with torch.no_grad():
        W = torch.abs(model.linear.weight)
        W /= L*torch.norm(model.linear.weight, p = 2)

    return W.detach().numpy().flatten()

def train_minibatch(model,optimizer,dataset,
                cos_sim,n_epochs,
                lam =0.5,
                batch_size = 128, spikes = False):
    loader = DataLoader(dataset,batch_size,shuffle = True)
    for i in range(n_epochs):
        for x,y in loader:
            if spikes:
                x = torch.nn.functional.softmax(x,dim =0)
                y = torch.nn.functional.softmax(y,dim =0)
            optimizer.zero_grad()
            pred = model(x)
            loss = 1 - cos_sim(pred,y)
            regloss = lam* torch.norm(model.linear.weight,p = 1)
            L = loss + regloss
            L.backward()
            optimizer.step()
    
    
    with torch.no_grad():
        W = torch.abs(model.linear.weight)
        W /= torch.norm(model.linear.weight, p = 2)

    return W.detach().numpy().flatten()


def get_scores_mb(
    model,
    dataset,
    n_epochs= 130,
    n_runs = 200,
    lam = 0.5,
    batch_size = 1024):
    
    scores = np.zeros(
        model.linear.weight.shape[1]
        )
    similarity = nn.CosineSimilarity(dim = 0,eps = 1e-8)

    for _ in tqdm(range(n_runs)):
        model.initialize_weights()
        optimizer = Adam(model.parameters(),lr = 1e-3)
        w = train_minibatch(
            model = model,
            optimizer = optimizer,
            dataset = dataset,
            cos_sim = similarity,
            n_epochs=n_epochs, lam = lam,
            batch_size = batch_size
        )
        scores +=w
    return scores

def get_scores(
                model,
                data,
                target,
                n_epochs = 130,
                n_runs = 200,lam = 0.5,):
    
    scores = np.zeros(
        model.linear.weight.shape[1]
        )
    similarity = nn.CosineSimilarity(dim = 0,eps = 1e-8)
    for _ in tqdm(range(n_runs)):
        model.initialize_weights()
        optimizer = Adam(model.parameters(),lr = 1e-3)
        w = train(
            model = model,
            optimizer = optimizer,
            data = data,
            target=target,
            cos_sim = similarity,
            n_epochs=n_epochs, lam = lam
        )
        scores +=w
    return scores

